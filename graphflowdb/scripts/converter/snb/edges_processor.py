import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class EdgesProcessor:
    edges_files = [
        "comment_hasCreator_person",
        "comment_hasTag_tag",
        "comment_isLocatedIn_place",
        "comment_replyOf_comment",
        "comment_replyOf_post",
        "forum_containerOf_post",
        "forum_hasMember_person",
        "forum_hasModerator_person",
        "forum_hasTag_tag",
        "organisation_isLocatedIn_place",
        # "person_email_emailAddress",
        "person_hasInterest_tag",
        "person_isLocatedIn_place",
        "person_knows_person",
        "person_likes_comment",
        "person_likes_post",
        # "person_speaks_language",
        "person_studyAt_organisation",
        "person_workAt_organisation",
        "place_isPartOf_place",
        "post_hasCreator_person",
        "post_hasTag_tag",
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

    @staticmethod
    def line_processors():
        return {
            "forum_hasMember_person": EdgesProcessor.date_processor,
            "person_knows_person": EdgesProcessor.date_processor,
            "person_likes_comment": EdgesProcessor.date_processor,
            "person_likes_post": EdgesProcessor.date_processor,
            "person_studyAt_organisation": EdgesProcessor.year_processor,
            "person_workAt_organisation": EdgesProcessor.year_processor
        }

    def __init__(self, vertex_map, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix
        self.vertex_map = vertex_map

    def convert(self):
        files = EdgesProcessor.edges_files
        headers = EdgesProcessor.file_headers
        processors = EdgesProcessor.line_processors()
        default_header = "FROM,TO"
        default_processor = EdgesProcessor.default_processor
        for file in files:
            name_parts = file.split("_")
            src_map = self.vertex_map[name_parts[0]]
            dst_map = self.vertex_map[name_parts[2]]
            processor = processors[file] if file in processors else default_processor
            header = headers[file] if file in headers else default_header
            self.processFile(file, header, src_map, dst_map, processor)
        total = "{:,}".format(self.count)
        logger.info("A total of {} edges were mapped.".format(total))

    def processFile(self, file_name, file_header, src_map, dst_map, line_processor):
        in_path = self.get_abs_in_path(file_name)
        out_path = self.get_abs_out_path(file_name)
        needs_header = not os.path.exists(out_path)
        with open(in_path, "r") as in_file, open(out_path, "a") as out_file:
            file_count = 0
            for i, line in enumerate(in_file):
                if i == 0:
                    if needs_header:
                        out_file.write(file_header + "\n")
                    continue
                parts = line.rstrip().split("|")
                src_id = str(src_map[int(parts[0])])
                dst_id = str(dst_map[int(parts[1])])
                output_line = line_processor(src_id, dst_id, parts)
                output_line = [x.replace(",", "|") for x in output_line]
                out_file.write(",".join(output_line) + "\n")
                self.count += 1
                file_count += 1
            number = "{:,}".format(file_count)
            logger.info("Mapped {} {} edges.".format(number, file_name))

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
        date = EdgesProcessor.get_unix_timestamp(parts[2])
        return [src_id, dst_id, date]

    @staticmethod
    def year_processor(src_id, dst_id, parts):
        return [src_id, dst_id, parts[2]]

    ################################# Helpers #################################
    @staticmethod
    def get_unix_timestamp(date_str):
        broken = date_str.split(".")
        date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
        return str(int(time.mktime(d_time.timetuple())))
