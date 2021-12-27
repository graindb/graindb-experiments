import os
import shutil
from .r2r_edges_processor import Relation2RelationEdgesProcessor
from .r2r_vertices_processor import RelationToRelationVerticesProcessor

# Changes:
# 1. Timestamp -> Integer

class RelationToRelationConverter:
    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix
        self.create_output_dir()

    def create_output_dir(self):
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir)

    def convert(self):
        RelationToRelationVerticesProcessor(self.input_dir, self.output_dir, self.post_fix).convert()
        Relation2RelationEdgesProcessor(self.input_dir, self.output_dir, self.post_fix).convert()
