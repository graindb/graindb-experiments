import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class Relation2GraphEdgesProcessor:
    edges_files = [
        "comment_hasTag_tag",
        "forum_hasMember_person",
        "forum_hasTag_tag",
        "person_hasInterest_tag",
        "person_knows_person",
        "person_likes_comment",
        "person_likes_post",
        "person_studyAt_organisation",
        "person_workAt_organisation",
        "post_hasTag_tag",
        # "person_speaks_language",
        # "person_email_emailAddress",

        "comment_hasCreator_person",
        "comment_isLocatedIn_place",
        "comment_replyOf_comment",
        "comment_replyOf_post",
        "forum_containerOf_post",
        "forum_hasModerator_person",
        "organisation_isLocatedIn_place",
        "person_isLocatedIn_place",
        "place_isPartOf_place",
        "post_hasCreator_person",
        "post_isLocatedIn_place",
        "tagclass_isSubclassOf_tagclass",
        "tag_hasType_tagclass"
    ]

    file_headers = {
        "forum_hasMember_person": "FROM,TO,date:INT",
        "person_knows_person": "FROM,TO,date:INT",
        "person_likes_comment": "FROM,TO,date:INT",
        "person_likes_post": "FROM,TO,date:INT",
        "person_studyAt_organisation": "FROM,TO,year:INT",
        "person_workAt_organisation": "FROM,TO,year:INT"
    }

    edge_output_input_map = {
        "comment_hasCreator_person": "comment",
        "comment_isLocatedIn_place": "comment",
        "comment_replyOf_comment": "comment",
        "comment_replyOf_post": "comment",
        "forum_containerOf_post": "post",
        "forum_hasModerator_person": "forum",
        "organisation_isLocatedIn_place": "organisation",
        "person_isLocatedIn_place": "person",
        "post_isLocatedIn_place": "post",
        "place_isPartOf_place": "place",
        "post_hasCreator_person": "post",
        "tagclass_isSubclassOf_tagclass": "tagclass",
        "tag_hasType_tagclass": "tag"
    }

    edge_src_col_indices = {
        "forum_containerOf_post": 9
    }

    edge_dst_col_indices = {
        "comment_hasCreator_person": 6,
        "comment_isLocatedIn_place": 7,
        "comment_replyOf_post": 8,
        "comment_replyOf_comment": 9,
        "forum_containerOf_post": 0,
        "forum_hasModerator_person": 3,
        "organisation_isLocatedIn_place": 4,
        "person_isLocatedIn_place": 8,
        "post_isLocatedIn_place": 10,
        "place_isPartOf_place": 4,
        "post_hasCreator_person": 8,
        "tagclass_isSubclassOf_tagclass": 3,
        "tag_hasType_tagclass": 3
    }

    default_header = "FROM,TO"

    @staticmethod
    def line_processors():
        return {
            "forum_hasMember_person": Relation2GraphEdgesProcessor.date_processor,
            "person_knows_person": Relation2GraphEdgesProcessor.date_processor,
            "person_likes_comment": Relation2GraphEdgesProcessor.date_processor,
            "person_likes_post": Relation2GraphEdgesProcessor.date_processor,
            "person_studyAt_organisation": Relation2GraphEdgesProcessor.year_processor,
            "person_workAt_organisation": Relation2GraphEdgesProcessor.year_processor
        }

    def __init__(self, vertex_map, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix
        self.vertex_map = vertex_map

    def convert(self):
        files = Relation2GraphEdgesProcessor.edges_files
        headers = Relation2GraphEdgesProcessor.file_headers
        processors = Relation2GraphEdgesProcessor.line_processors()
        default_processor = Relation2GraphEdgesProcessor.default_processor
        edge_output_input_map = Relation2GraphEdgesProcessor.edge_output_input_map
        edge_src_col_indices = Relation2GraphEdgesProcessor.edge_src_col_indices
        edge_dst_col_indices = Relation2GraphEdgesProcessor.edge_dst_col_indices
        for file in files:
            name_parts = file.split("_")
            src_map = self.vertex_map[name_parts[0]]
            dst_map = self.vertex_map[name_parts[2]]
            processor = processors[file] if file in processors else default_processor
            header = headers[file] if file in headers else self.default_header
            input_file = edge_output_input_map[file] if file in edge_output_input_map else file
            src_col_idx = edge_src_col_indices[file] if file in edge_src_col_indices else 0
            dst_col_idx = edge_dst_col_indices[file] if file in edge_dst_col_indices else 1
            print("> Processing edges", file, "...")
            self.processFile(input_file, file, header, src_map, dst_map, processor, src_col_idx, dst_col_idx)
        total = "{:,}".format(self.count)
        logger.info("A total of {} edges were mapped.".format(total))

    def processFile(self, input_file_name, output_file_name, file_header, src_map, dst_map, line_processor, src_col_idx, dst_col_idx):
        in_path = self.get_abs_in_path(input_file_name)
        out_path = self.get_abs_out_path(output_file_name)
        needs_header = not os.path.exists(out_path)
        with open(in_path, "r") as in_file, open(out_path, "a") as out_file:
            file_count = 0
            for i, line in enumerate(in_file):
                if i == 0:
                    if needs_header:
                        out_file.write(file_header + "\n")
                    continue
                parts = line.rstrip().split("|")
                if parts[src_col_idx] == '' or parts[dst_col_idx] == '':
                    continue
                src_id = str(src_map[int(parts[src_col_idx])])
                dst_id = str(dst_map[int(parts[dst_col_idx])])
                output_line = line_processor(src_id, dst_id, parts)
                output_line = [x.replace(",", "|") for x in output_line]
                out_file.write(",".join(output_line) + "\n")
                self.count += 1
                file_count += 1
            number = "{:,}".format(file_count)
            logger.info("Mapped {} {} edges.".format(number, output_file_name))

    def get_abs_in_path(self, file_name):
        return os.path.join(self.input_dir, file_name + self.post_fix)

    def get_abs_out_path(self, file_name):
        name = "e-" + file_name.split("_")[1] + ".csv"
        return os.path.join(self.output_dir, name)

    ############################# Line Processors #############################

    @staticmethod
    def default_processor(src_id, dst_id, parts):
        return [src_id, dst_id]

    @staticmethod
    def date_processor(src_id, dst_id, parts):
        date = Relation2GraphEdgesProcessor.get_unix_timestamp(parts[2])
        return [src_id, dst_id, date]

    @staticmethod
    def year_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[2]]

    ################################# Helpers #################################
    @staticmethod
    def get_unix_timestamp(date_str):
        broken = date_str.split(".")
        #date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(broken[0], '%Y-%m-%d %H:%M:%S')
        return str(int(time.mktime(d_time.timetuple())))
