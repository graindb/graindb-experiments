import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class RelationToRelationVerticesProcessor:
    vertex_files = [
        "comment",
        "forum",
        "organisation",
        "person",
        "place",
        "post",
        "tag",
        "tagclass"
    ]

    file_headers = [
        # comment
        ("id,c_id,vid,c_creationdate,c_locationip,c_browserused,c_content,c_length,c_creatorid,c_locationid,"
         "c_replyof_post,c_replyof_comment"),
        # forum
        "id,f_forumid,vid,f_title,f_creationDate,f_moderatorid",
        # organization
        "id,o_organisationid,vid,o_type,o_name,o_url,o_placeid",
        # person
        ("id,p_personid,vid,p_firstname,p_lastname,p_gender,p_birthday,p_creationDate,p_locationip,p_browserused,"
         "p_placeid"),
        # place
        "id,pl_placeid,vid,pl_name,pl_url,pl_type,pl_containerplaceid",
        # post
        ("id,ps_id,vid,ps_imagefile,ps_creationdate,ps_locationip,ps_browserused,ps_language,ps_content,ps_length,"
         "ps_creatorid,m_ps_forumid,ps_locationid"),
        # tag
        "id,t_tagid,vid,t_name,t_url,t_tagclassid",
        # tag class
        "id,tc_tagclassid,vid,tc_name,tc_url,tc_subclassoftagclassid"
    ]

    @staticmethod
    def line_processors():
        return [
            # comment
            RelationToRelationVerticesProcessor.process_comment_line,
            # forum
            RelationToRelationVerticesProcessor.process_forum,
            # organisation
            RelationToRelationVerticesProcessor.process_organisation,
            # person
            RelationToRelationVerticesProcessor.process_person,
            # place
            RelationToRelationVerticesProcessor.process_place,
            # post
            RelationToRelationVerticesProcessor.process_post,
            # tag
            RelationToRelationVerticesProcessor.process_tag,
            # tagclass
            RelationToRelationVerticesProcessor.process_tagclass
        ]

    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix

    def convert(self):
        files = RelationToRelationVerticesProcessor.vertex_files
        headers = RelationToRelationVerticesProcessor.file_headers
        processors = RelationToRelationVerticesProcessor.line_processors()
        for i in range(len(files)):
            print("> Processing vertices", files[i], "...")
            self.process_file(files[i], headers[i], processors[i])
        total = "{:,}".format(self.count)
        logger.info("A total of {} vertices were mapped.".format(total))

    def process_file(self, file_name, file_header, line_processor):
        in_path = self.get_abs_in_path(file_name)
        out_path = self.get_abs_out_path(file_name)
        with open(in_path, "r") as in_file, open(out_path, "w") as out_file:
            for i, line in enumerate(in_file):
                if i == 0:
                    out_file.write(file_header + "\n")
                    continue
                parts = line.rstrip().split("|")
                output_line = line_processor(str(self.count), parts)
                output_line = [x.replace(",", "|") for x in output_line]
                out_file.write(",".join(output_line) + "\n")
                self.count += 1
            logger.info("Mapped {} vertices.".format(file_name))

    def get_abs_in_path(self, file_name):
        return os.path.join(self.input_dir, file_name + self.post_fix)

    def get_abs_out_path(self, file_name):
        name = file_name + ".csv"
        return os.path.join(self.output_dir, name)

    ############################# Line Processors #############################

    @staticmethod
    def process_comment_line(v_id, parts):
        creation_date = RelationToRelationVerticesProcessor.get_unix_timestamp(parts[1])
        browser = RelationToRelationVerticesProcessor.map_browser(parts[3])
        return [parts[0], parts[0], v_id, creation_date, parts[2], browser, parts[4], parts[5], parts[6], parts[7],
                parts[8], parts[9]]

    @staticmethod
    def process_forum(v_id, parts):
        id, title, moderator = parts[0], parts[1], parts[3]
        creation_date = RelationToRelationVerticesProcessor.get_unix_timestamp(parts[2])
        return [id, id, v_id, title, creation_date, moderator]

    @staticmethod
    def process_organisation(v_id, parts):
        id, url, place = parts[0], parts[3], parts[4]
        o_type = "1" if parts[1] == "company" else "2"
        name = parts[2]
        return [id, id, v_id, o_type, name, url, place]

    @staticmethod
    def process_person(v_id, parts):
        id, f_name, l_name, birthday, ip, place = parts[0], parts[1], parts[2], parts[4], parts[6], parts[8]
        gender = "1" if parts[3] == "male" else "2"
        creation_date = RelationToRelationVerticesProcessor.get_unix_timestamp(parts[5])
        browser = RelationToRelationVerticesProcessor.map_browser(parts[7])
        return [id, id, v_id, f_name, l_name, gender, birthday, creation_date, ip, browser, place]

    @staticmethod
    def process_place(v_id, parts):
        id, name, url, container = parts[0], parts[1], parts[2], parts[4]
        p_type = RelationToRelationVerticesProcessor.map_place_type(parts[3])
        return [id, id, v_id, name, url, p_type, container]

    @staticmethod
    def process_post(v_id, parts):
        id, image, ip, language, content, length, creator, location, forum = parts[0], parts[1], \
                             parts[3], parts[5], parts[6], parts[7], parts[8], parts[9], parts[10]
        browser = RelationToRelationVerticesProcessor.map_browser(parts[4])
        c_date = RelationToRelationVerticesProcessor.get_unix_timestamp(parts[2])
        return [id, id, v_id, image, c_date, ip, browser, language, content, length, creator, location, forum]

    @staticmethod
    def process_tag(v_id, parts):
        id, name, url, tagclass = parts[0], parts[1], parts[2], parts[3]
        return [id, id, v_id, name, url, tagclass]

    @staticmethod
    def process_tagclass(v_id, parts):
        id, name, url, subclass = parts[0], parts[1], parts[2], parts[3]
        return [id, id, v_id, name, url, subclass]

    ################################# Helpers #################################

    @staticmethod
    def map_place_type(string):
        if string == "city":
            return "1"
        if string == "country":
            return "2"
        if string == "continent":
            return "3"
        raise ValueError("Invalid place type found.")

    @staticmethod
    def map_browser(string):
        if string == "Chrome":
            return "1"
        if string == "Firefox":
            return "2"
        if string == "Internet Explorer":
            return "3"
        if string == "Safari":
            return "4"
        if string == "Opera":
            return "5"
        print(string)
        raise ValueError("Invalid browser type found.")

    @staticmethod
    def get_unix_timestamp(date_str):
        broken = date_str.split(".")
        #date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(broken[0], '%Y-%m-%d %H:%M:%S')
        return str(int(time.mktime(d_time.timetuple())))
