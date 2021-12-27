import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class RelationToGraphVerticesProcessor:
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
        "id,cid:INT,oid:STRING,creationDate:INT,locationIP:STRING,browserUsed:INT,content:STRING,length:INT",
        # forum
        "id,cid:INT,oid:STRING,title:STRING,creationDate:INT",
        # organization
        "id,cid:INT,oid:STRING,type:INT,name:STRING,url:STRING",
        # person
        ("id,cid:INT,oid:STRING,fName:STRING,lName:STRING,gender:INT,birthday:STRING,"
         "creationDate:INT,ip:STRING,browserUsed:INT"),
        # place
        "id,cid:INT,oid:STRING,name:STRING,url:STRING,type:INT",
        # post
        ("id,cid:INT,oid:STRING,imageFile:STRING,creationDate:INT,locationIP:STRING,"
         "browserUsed:INT,language:STRING,content:STRING,length:INT"),
        # tag
        "id,cid:INT,oid:STRING,name:STRING,url:STRING",
        # tag class
        "id,cid:INT,oid:STRING,name:STRING,url:STRING"
    ]

    @staticmethod
    def line_processors():
        return [
            # comment
            RelationToGraphVerticesProcessor.process_comment_line,
            # forum
            RelationToGraphVerticesProcessor.process_forum,
            # organisation
            RelationToGraphVerticesProcessor.process_organisation,
            # person
            RelationToGraphVerticesProcessor.process_person,
            # place
            RelationToGraphVerticesProcessor.process_place,
            # post
            RelationToGraphVerticesProcessor.process_post,
            # tag
            RelationToGraphVerticesProcessor.process_tag,
            # tagclass
            RelationToGraphVerticesProcessor.process_tagclass
        ]

    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.global_map = dict()
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix

    def convert(self):
        files = RelationToGraphVerticesProcessor.vertex_files
        headers = RelationToGraphVerticesProcessor.file_headers
        processors = RelationToGraphVerticesProcessor.line_processors()
        for i in range(len(files)):
            print("> Processing vertices", files[i], "...")
            self.process_file(files[i], headers[i], processors[i])
        total = "{:,}".format(self.count)
        logger.info("A total of {} vertices were mapped.".format(total))
        return self.global_map

    def process_file(self, file_name, file_header, line_processor):
        in_path = self.get_abs_in_path(file_name)
        out_path = self.get_abs_out_path(file_name)
        vertices_map = dict()
        self.global_map[file_name] = vertices_map
        with open(in_path, "r") as in_file, open(out_path, "w") as out_file:
            for i, line in enumerate(in_file):
                if i == 0:
                    out_file.write(file_header + "\n")
                    continue
                parts = line.rstrip().split("|")
                vertices_map[int(parts[0])] = self.count
                output_line = line_processor(str(self.count), parts)
                output_line = [x.replace(",", "|") for x in output_line]
                out_file.write(",".join(output_line) + "\n")
                self.count += 1
            number = "{:,}".format(len(vertices_map))
            logger.info("Mapped {} {} vertices.".format(number, file_name))

    def get_abs_in_path(self, file_name):
        return os.path.join(self.input_dir, file_name + self.post_fix)

    def get_abs_out_path(self, file_name):
        name = "v-" + file_name + ".csv"
        return os.path.join(self.output_dir, name)

    ############################# Line Processors #############################

    @staticmethod
    def process_comment_line(v_id, parts):
        creation_date = RelationToGraphVerticesProcessor.get_unix_timestamp(parts[1])
        browser = RelationToGraphVerticesProcessor.map_browser(parts[3])
        oid, ip, content, length = parts[0], parts[2], parts[4], parts[5]
        return [v_id, v_id, oid, creation_date, ip, browser, content, length]

    @staticmethod
    def process_forum(v_id, parts):
        oid, title = parts[0], parts[1]
        creation_date = RelationToGraphVerticesProcessor.get_unix_timestamp(parts[2])
        return [v_id, v_id, oid, title, creation_date]

    @staticmethod
    def process_organisation(v_id, parts):
        o_type = "1" if parts[1] == "company" else "2"
        oid, url = parts[0], parts[2]
        name = parts[2]
        return [v_id, v_id, oid, o_type, name, url]

    @staticmethod
    def process_person(v_id, parts):
        oid, f_name, l_name, birthday, ip = parts[0], parts[1], parts[2], parts[4], parts[6]
        gender = "1" if parts[3] == "male" else "2"
        browser = RelationToGraphVerticesProcessor.map_browser(parts[7])
        creation_date = RelationToGraphVerticesProcessor.get_unix_timestamp(parts[5])
        return [v_id, v_id, oid, f_name, l_name, gender, birthday, creation_date, ip, browser]

    @staticmethod
    def process_place(v_id, parts):
        oid, name, url = parts[0], parts[1], parts[2]
        p_type = RelationToGraphVerticesProcessor.map_place_type(parts[3])
        return [v_id, v_id, oid, name, url, p_type]

    @staticmethod
    def process_post(v_id, parts):
        oid, image, ip, language, content, length = parts[0], parts[1], parts[3], parts[5], parts[6], parts[7]
        browser = RelationToGraphVerticesProcessor.map_browser(parts[4])
        c_date = RelationToGraphVerticesProcessor.get_unix_timestamp(parts[2])
        return [v_id, v_id, oid, image, c_date, ip, browser, language, content, length]

    @staticmethod
    def process_tag(v_id, parts):
        oid, name, url = parts[0], parts[1], parts[2]
        return [v_id, v_id, oid, name, url]

    @staticmethod
    def process_tagclass(v_id, parts):
        oid, name, url = parts[0], parts[1], parts[2]
        return [v_id, v_id, oid, name, url]

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
