import os
import shutil
from .job_r2g_edges_processor import JobRelationToGraphEdgesProcessor
from .job_r2g_vertices_processor import JobRelationToGraphVerticesProcessor


# Changes:
# 1. comment and post are merged as message
# 2. hasTag is divided into forumHasTag and messageHasTag
# 3. isLocatedIn is divided into messageIsLocatedIn, personIsLocatedIn, forumIsLocatedIn

class RelationToGraphConverter:
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
        global_map = JobRelationToGraphVerticesProcessor(self.input_dir,
                                                      self.output_dir, self.post_fix).convert()
        JobRelationToGraphEdgesProcessor(global_map, self.input_dir,
                                     self.output_dir, self.post_fix).convert()
