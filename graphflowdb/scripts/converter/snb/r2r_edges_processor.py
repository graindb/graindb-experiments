import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class Relation2RelationEdgesProcessor:
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
    ]

    file_headers = {
        "comment_hasTag_tag": "mt_messageid,mt_tagid",
        "forum_hasMember_person": "fp_forumid,fp_personid,fp_joindate",
        "forum_hasTag_tag": "ft_forumid,ft_tagid",
        "person_hasInterest_tag": "pt_personid,pt_tagid",
        "person_knows_person": "k_person1id,k_person2id,k_creationdate",
        "person_likes_comment": "l_personid,l_commentid,l_creationdate",
        "person_likes_post": "l_personid,l_postid,l_creationdate",
        "person_studyAt_organisation": "pu_personid,pu_organisationid,pu_classyear",
        "person_workAt_organisation": "pc_personid,pc_organisationid,pc_workfrom",
        "post_hasTag_tag": "mt_messageid,mt_tagid"
    }

    output_files_map = {
        "comment_hasTag_tag": "comment_tag",
        "forum_hasMember_person": "forum_person",
        "forum_hasTag_tag": "forum_tag",
        "person_hasInterest_tag": "person_tag",
        "person_knows_person": "knows",
        "person_likes_comment": "likes_comment",
        "person_likes_post": "likes_post",
        "person_studyAt_organisation": "person_university",
        "person_workAt_organisation": "person_company",
        "post_hasTag_tag": "post_tag"
    }

    default_header = "from,to"

    @staticmethod
    def line_processors():
        return {
            "forum_hasMember_person": Relation2RelationEdgesProcessor.date_processor,
            "person_knows_person": Relation2RelationEdgesProcessor.date_processor,
            "person_likes_comment": Relation2RelationEdgesProcessor.date_processor,
            "person_likes_post": Relation2RelationEdgesProcessor.date_processor,
            "person_studyAt_organisation": Relation2RelationEdgesProcessor.year_processor,
            "person_workAt_organisation": Relation2RelationEdgesProcessor.year_processor
        }

    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix

    def convert(self):
        files = Relation2RelationEdgesProcessor.edges_files
        headers = Relation2RelationEdgesProcessor.file_headers
        processors = Relation2RelationEdgesProcessor.line_processors()
        default_processor = Relation2RelationEdgesProcessor.default_processor
        for file in files:
            processor = processors[file] if file in processors else default_processor
            header = headers[file] if file in headers else self.default_header
            print("> Processing edges", file, "...")
            self.processFile(file, header, processor)
        total = "{:,}".format(self.count)
        logger.info("A total of {} edges were mapped.".format(total))

    def processFile(self, file_name, file_header, line_processor):
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
                output_line = line_processor(parts)
                output_line = [x.replace(",", "|") for x in output_line]
                out_file.write(",".join(output_line) + "\n")
                self.count += 1
                file_count += 1
            number = "{:,}".format(file_count)
            logger.info("Mapped {} {} edges.".format(number, file_name))

    def get_abs_in_path(self, file_name):
        return os.path.join(self.input_dir, file_name + self.post_fix)

    def get_abs_out_path(self, file_name):
        output_name = Relation2RelationEdgesProcessor.output_files_map[file_name] \
            if file_name in Relation2RelationEdgesProcessor.output_files_map else file_name
        name = output_name + ".csv"
        return os.path.join(self.output_dir, name)

    ############################# Line Processors #############################

    @staticmethod
    def default_processor(parts):
        return parts

    @staticmethod
    def date_processor(parts):
        date = Relation2RelationEdgesProcessor.get_unix_timestamp(parts[2])
        return [parts[0], parts[1], date]

    @staticmethod
    def year_processor(parts):
        return [parts[0], parts[1], parts[2]]

    ################################# Helpers #################################
    @staticmethod
    def get_unix_timestamp(date_str):
        broken = date_str.split(".")
        #date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(broken[0], '%Y-%m-%d %H:%M:%S')
        return str(int(time.mktime(d_time.timetuple())))
