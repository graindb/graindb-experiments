import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class JobRelationToGraphEdgesProcessor:
    edges_files = [
        "movie_keyword",  # keyword-[]-title
        "movie_info_idx",  # info_type-[]-title
        "movie_info",  # info_type-[]-title
        "person_info",  # info_type-[]-name
        "t_kind_type",  # title-[]-kind_type
        "cn_movie_companies",  # company_name-[]-title
        "ct_movie_companies",  # company_type-[]-title
        "movie_link",  # link_type-[]-title
        "at_title",  # aka_title-[]-title
        "an_name",  # aka_name-[]-name
        "rt_cast_info",  # role_type-[]-title
        "cn_cast_info",  # char_name-[]-title
        "an_cast_info",  # aka_name-[]-title
        "n_cast_info",   # name-[]-title
        "subject_cc_type",  # comp_cast_type-[]-title (join on subject)
        "status_cc_type",  # comp_cast_type-[]-title (join on status)
    ]

    file_headers = {
        "movie_keyword": "FROM,TO,tid:INT",
        "movie_info_idx": "FROM,TO,info:STRING,tid:INT,info:STRING,note:STRING",
        "movie_info": "FROM,TO,info:STRING,tid:INT,info:STRING,note:STRING",
        "person_info": "FROM,TO,info:STRING,tid:INT,note:STRING",
        "cn_movie_companies": "FROM,TO,tid:INT",
        "ct_movie_companies": "FROM,TO,tid:INT",
        "movie_link": "FROM,TO,tid:INT",
        "rt_cast_info": "FROM,TO,tid:INT,note:STRING,nr_order:INT",
        "cn_cast_info": "FROM,TO,tid:INT,note:STRING,nr_order:INT",
        "an_cast_info": "FROM,TO,tid:INT,note:STRING,nr_order:INT",
        "n_cast_info": "FROM,TO,tid:INT,note:STRING,nr_order:INT",
        "subject_cc_type": "FROM,TO,tid:INT",
        "status_cc_type": "FROM,TO,tid:INT"
    }

    edge_output_input_map = {
        "movie_keyword": "movie_keyword",
        "movie_info_idx": "movie_info_idx",
        "movie_info": "movie_info",
        "person_info": "person_info",
        "t_kind_type": "title",
        "cn_movie_companies": "movie_companies",
        "ct_movie_companies": "movie_companies",
        "movie_link": "movie_link",
        "at_title": "aka_title",
        "an_name": "aka_name",
        "rt_cast_info": "cast_info",
        "cn_cast_info": "cast_info",
        "an_cast_info": "cast_info",
        "n_cast_info": "cast_info",
        "subject_cc_type": "complete_cast",
        "status_cc_type": "complete_cast"
    }

    edge_src_col_indices = {
        "movie_keyword": 2,
        "movie_info_idx": 2,
        "movie_info": 2,
        "person_info": 2,
        "t_kind_type": 0,
        "cn_movie_companies": 2,
        "ct_movie_companies": 3,
        "movie_link": 3,
        "at_title": 0,
        "an_name": 0,
        "rt_cast_info": 6,
        "cn_cast_info": 3,
        "an_cast_info": 1,
        "n_cast_info": 1,
        "subject_cc_type": 2,
        "status_cc_type": 3
    }

    edge_dst_col_indices = {
        "movie_keyword": 1,
        "movie_info_idx": 1,
        "movie_info": 1,
        "person_info": 1,
        "t_kind_type": 3,
        "cn_movie_companies": 1,
        "ct_movie_companies": 1,
        "movie_link": 1,
        "at_title": 1,
        "an_name": 1,
        "rt_cast_info": 2,
        "cn_cast_info": 2,
        "an_cast_info": 2,
        "n_cast_info": 2,
        "subject_cc_type": 1,
        "status_cc_type": 1
    }

    default_header = "FROM,TO"

    @staticmethod
    def line_processors():
        return {
            "movie_keyword": JobRelationToGraphEdgesProcessor.default_tid_processor,
            "cn_movie_companies": JobRelationToGraphEdgesProcessor.default_tid_processor,
            "ct_movie_companies": JobRelationToGraphEdgesProcessor.default_tid_processor,
            "movie_link": JobRelationToGraphEdgesProcessor.default_tid_processor,
            "subject_cc_type": JobRelationToGraphEdgesProcessor.default_tid_processor,
            "status_cc_type": JobRelationToGraphEdgesProcessor.default_tid_processor,

            "movie_info_idx": JobRelationToGraphEdgesProcessor.movie_info_processor,
            "movie_info": JobRelationToGraphEdgesProcessor.movie_info_processor,
            "person_info": JobRelationToGraphEdgesProcessor.person_info_processor,
            "rt_cast_info": JobRelationToGraphEdgesProcessor.cast_info_processor,
            "cn_cast_info": JobRelationToGraphEdgesProcessor.cast_info_processor,
            "an_cast_info": JobRelationToGraphEdgesProcessor.cast_info_processor,
        }

    def __init__(self, vertex_map, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix
        self.vertex_map = vertex_map

    def convert(self):
        files = JobRelationToGraphEdgesProcessor.edges_files
        headers = JobRelationToGraphEdgesProcessor.file_headers
        processors = JobRelationToGraphEdgesProcessor.line_processors()
        default_processor = JobRelationToGraphEdgesProcessor.default_processor
        edge_output_input_map = JobRelationToGraphEdgesProcessor.edge_output_input_map
        edge_src_col_indices = JobRelationToGraphEdgesProcessor.edge_src_col_indices
        edge_dst_col_indices = JobRelationToGraphEdgesProcessor.edge_dst_col_indices
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

    def processFile(self, input_file_name, output_file_name, file_header, src_map, dst_map, line_processor, src_col_idx,
                    dst_col_idx):
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
    def default_tid_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[0]]

    @staticmethod
    def cast_info_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[0], parts[4], parts[5]]

    @staticmethod
    def movie_info_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[0], parts[3], parts[4]]

    @staticmethod
    def person_info_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[0], parts[3], parts[4]]

    @staticmethod
    def date_processor(src_id, dst_id, parts):
        date = JobRelationToGraphEdgesProcessor.get_unix_timestamp(parts[2])
        return [src_id, dst_id, date]

    @staticmethod
    def year_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[2]]

    ################################# Helpers #################################
    @staticmethod
    def get_unix_timestamp(date_str):
        broken = date_str.split(".")
        # date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(broken[0], '%Y-%m-%d %H:%M:%S')
        return str(int(time.mktime(d_time.timetuple())))
