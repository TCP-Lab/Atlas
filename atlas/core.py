from atlas.abcs import AtlasInterface


class Atlas:
    def __init__(self) -> None:
        self.interfaces: list[AtlasInterface] = []
