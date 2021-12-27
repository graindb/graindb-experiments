import os
import shutil
from .edges_processor import EdgesProcessor
from .vertices_processor import VerticesProcessor


class GraphToGraphConverter:
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
        global_map = VerticesProcessor(self.input_dir,
                                       self.output_dir, self.post_fix).convert()
        EdgesProcessor(global_map, self.input_dir,
                       self.output_dir, self.post_fix).convert()
