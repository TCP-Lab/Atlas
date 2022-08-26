"""Interface module, containing all interfaces for Atlas.

###################   >>>  INTERFACE BEST PRACTICES  <<<    ####################
1. Try and reuse old downloaders and processors, if possible.
2. Keep in mind that errors do not get raised immediately. Instead, the
   interface catches them and returns them to the main process. It is after
   all processes complete than Atlas checks to see if any processes have raised
   an error and then re-raises it. This is similar to what `ProcessPoolExecutor`
   already does, but the stack trace is smaller, and we can get what interface
   died with what error, and log it.
3. Keep in mind that the pool is not CTRL+C friendly. The pool catches the
   interrupt, stops making new children, waits until they are all finished
   or dead, and *then* stops, re-raising the KeyboardInterrupt.
   If you really want, use CTRL-C twice to kill the handler. But this might leave
   orphaned processes.
4. Interfaces should all return a pd.DataFrame with the downloaded and
   (optionally) processed data. These dataframes should all contain a single
   "pivot" column that depends on the type of the interface. Atlas will
   automatically detect this common column, and perform a per-row merge
   of the dataframes with this single pivot column. The dataframe is then
   saved to disk.
5. It is YOUR RESPONSIBILITY to ensure the following:
    - The interface names MUST be unique. This is because the queries rely on
      the interface names to be fulfilled.
    - Interfaces MUST fulfill their `.provided_cols` promise. Atlas will trigger
      a warning (not an error) if this is not the case.
6. Interfaces not added to the ALL_INTERFACES variable will not be loaded by
   Atlas. This is a good way to remove test interfaces from Atlas while not
   developing the tool.
"""
import json
import logging

# TODO: Add some way to test point n. 5 above in automated deployment tests.
from functools import partial, reduce

import pandas as pd
from tqdm import tqdm

from cma_atlas import OPTIONS, abcs
from cma_atlas.test_interfaces import ALL_TEST_INTERFACES
from cma_atlas.utils.constants import TCGA_CANCER_TYPES
from cma_atlas.utils.tools import download_as_bytes_with_progress

log = logging.getLogger(__name__)

# Specify which interfaces are part of Atlas.
ALL_INTERFACES = []

down_tqdm = partial(tqdm, leave=False, colour="BLUE")
process_tqdm = partial(tqdm, leave=False, colour="GREEN")

################################################################################
##########################   TCGA METADATA    ##################################
################################################################################


class TCGAMetadataDownloader(abcs.AtlasDownloader):
    cases_endpt = "https://api.gdc.cancer.gov/cases/"

    def __init__(self, cancer_type):
        self.cancer_type = cancer_type
        self.project_id = f"TCGA-{cancer_type}"

    def retrieve(self, name: str):
        filters = {
            "op": "in",
            "content": {"field": "project.project_id", "value": [self.project_id]},
        }

        result = {}
        for data_type in ("demographic", "diagnoses", "exposures"):
            log.debug(f"Retrieving data for {self.cancer_type} - {data_type}")
            params = {
                "filters": json.dumps(filters),
                "format": "JSON",
                "expand": data_type,
                # This is the max request size
                "size": str(100_000_000),
            }

            log.debug("Decoding response for {self.cancer_type} - {data_type}...")
            response = download_as_bytes_with_progress(
                self.cases_endpt,
                params=params,
                tqdm_class=partial(
                    down_tqdm,
                    position=self.worker_id,
                    desc=f"{self.cancer_type} - {data_type}",
                ),
            )

            result[data_type] = json.loads(response.decode("UTF-8"))["data"]["hits"]

        log.debug(f"Done retrieving data for {self.cancer_type}.")
        return result


class TCGAMetadataProcessor(abcs.AtlasProcessor):
    def __init__(self, cancer_type):
        self.cancer_type = cancer_type

    def __call__(self, name: str, melted_data) -> pd.DataFrame:
        # The melted data is the three responses all bundled together.
        # They need to be cleaned and merged to just one Df.
        dataframes = []
        for data_type in process_tqdm(
            ("demographic", "diagnoses", "exposures"),
            position=self.worker_id,
            desc=f"Process {name}",
        ):
            log.info("Detecting missing patient data...")
            missing_diagnoses = []
            cases = []
            for patient in melted_data[data_type]:
                clean_data = {}
                try:
                    if data_type != "demographic":
                        # Diagnoses and exposures are in a list of 1 element, so
                        # I'm unlisting them here (the [0])
                        clean_data.update(patient[data_type][0])
                    else:
                        # Demographic is just a dictionary, no need to unlist
                        clean_data.update(patient[data_type])
                except KeyError:
                    missing_diagnoses.append(patient["submitter_id"])
                # Add the relevant patient ID to the cleaned data for merging
                clean_data.update({"submitter_id": patient["submitter_id"]})
                cases.append(clean_data)
            # Warn the user if something went wrong when retrieving the data
            if missing_diagnoses:
                str_missing_diagnoses = ", ".join(missing_diagnoses)
                log.warning(
                    f"Found one or more missing {data_type}: {str_missing_diagnoses}"
                )
            # Delete the variables "updated_datetime", "created_datetime" and "state". They are basically useless.
            frame = pd.DataFrame(cases)
            log.debug("Deleting useless columns...")
            frame.drop(
                columns=["state", "updated_datetime", "created_datetime"],
                inplace=True,
                errors="ignore",
            )
            # Finally, add the dataframe to the dataframe list
            dataframes.append(frame)

        log.debug("Merging frames...")
        merged_frame = reduce(
            lambda x, y: pd.merge(x, y, on="submitter_id", how="outer"),
            process_tqdm(dataframes, position=self.worker_id - 1, desc=f"{name}"),
        )

        log.debug("Standardizing TCGA notations...")
        merged_frame.replace(
            ["", "not reported", "Not Reported", "NA", "na", "Na"], None, inplace=True
        )
        merged_frame.replace(["No", "NO", "no"], False, inplace=True)
        merged_frame.replace(["Yes", "yes", "YES"], True, inplace=True)

        log.debug("Adding TCGA ID column")
        merged_frame["tumor_id"] = self.cancer_type

        log.debug("Dropping empty columns...")
        merged_frame.dropna(axis=1, how="all", inplace=True)
        log.info(f"Returning data of type {type(merged_frame)}")
        return merged_frame


tcga_metadata_interfaces = []
for cancer_type in TCGA_CANCER_TYPES:
    # There are many cancer types, so we do this in a loop, not with
    # individual classes
    interface = abcs.AtlasInterface()
    interface.type = "TCGA Clinical Metadata"
    interface.name = f"TCGA-{cancer_type} Metadata"
    interface.downloader = TCGAMetadataDownloader(cancer_type)
    interface.provided_cols = None  # TODO: Fillme
    interface.processor = TCGAMetadataProcessor(cancer_type)
    interface.merge_col = "submitter_id"
    tcga_metadata_interfaces.append(interface)

ALL_INTERFACES.extend(tcga_metadata_interfaces)

###############################################################################

# Add test interfaces if we are in debug mode
if OPTIONS["debugging"]["include_test_interfaces"]:
    ALL_INTERFACES.extend(ALL_TEST_INTERFACES)
###############################################################################
