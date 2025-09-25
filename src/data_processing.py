#FILE NOW REDUNDANT FOR THIS REPOSITORY. WILL BE FORKED FOR OTHER PROJECT!
"""from typing import Optional, List, Dict

class VectorID:
    def __init__(self, vector_id: int, product_id: int, definition: str):
        self.vector_id = vector_id
        self.table_number = product_id
        self.definition = definition
        self.series = Dict[str, float]
        self.parent: Optional['VectorID'] = None
        self.children: List['VectorID'] = []

    def add_child(self, child: 'VectorID'):
        child.parent = self
        self.children.append(child)

    def add_series_data(self, series_data: Dict[str, float]):
        self.series.update(series_data)

    def __repr__(self) -> str:
        parent_id = self.parent.vector_id if self.parent else None
        return (f'VectorID(table={self.table_number}, vector={self.vector_id}, '
                f'parent={parent_id})')

class ProductID:
    def __init__(self, product_id: int, definition: str):
        self.product_id = product_id
        self.definition = definition
        self.roots: List['VectorID'] = []
        self._vector_map: Dict[int, VectorID] = {}

    def add_root_vector(self, vector_id: int, definition: str) -> VectorID:
        vector = VectorID(vector_id, self.product_id, definition)
        self.roots.append(vector)
        self._vector_map[vector.vector_id] = vector
        return vector

    def find_vector(self, vector_id: int) -> Optional[VectorID]:
        return self._vector_map.get(vector_id, None)

    def __repr__(self) -> str:
        return (f"Table Number = {self.product_id}, "
                f"definition={self.definition}, "
                f"roots={len(self.roots)}")
"""