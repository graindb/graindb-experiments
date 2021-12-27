import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class VerticesProcessor:
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
        "id,creationDate:INT,locationIP:STRING,length:INT",
        # forum
        "id,title:STRING,creationDate:INT",
        # organization
        "id,type:INT,name:STRING",
        # person
        ("id,fName:STRING,lName:STRING,gender:INT,bYear:INT,"
         "bMonth:INT,bDay:INT,creationDate:INT"),
        # place
        "id,name:STRING,type:INT",
        # post
        ("id,imageFile:STRING,creationDate:INT,locationIP:STRING,"
        "browserUsed:INT,language:STRING,content:STRING,length:INT"),
        # tag
        "id,name:STRING",
        # tag class
        "id,name:STRING"
    ]

    @staticmethod
    def line_processors():
        return [
            # comment
            VerticesProcessor.process_comment_line,
            # forum
            VerticesProcessor.process_forum,
            # organisation
            VerticesProcessor.process_organisation,
            # person
            VerticesProcessor.process_person,
            # place
            VerticesProcessor.process_place,
            # post
            VerticesProcessor.process_post,
            # tag
            VerticesProcessor.process_tag,
            # tagclass
            VerticesProcessor.process_tagclass
        ]

    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.global_map = dict()
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix

    def convert(self):
        files = VerticesProcessor.vertex_files
        headers = VerticesProcessor.file_headers
        processors = VerticesProcessor.line_processors()
        for i in range(len(files)):
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
        creation_date = VerticesProcessor.get_unix_timestamp(parts[1])
        ip, length = parts[2], parts[5]
        return [v_id, creation_date, ip, length]

    @staticmethod
    def process_forum(v_id, parts):
        title = parts[1]
        creation_date = VerticesProcessor.get_unix_timestamp(parts[2])
        return [v_id, title, creation_date]

    @staticmethod
    def process_organisation(v_id, parts):
        o_type = "1" if parts[1] == "company" else "2"
        name = parts[2]
        return [v_id, o_type, name]

    @staticmethod
    def process_person(v_id, parts):
        f_name = parts[1]
        l_name = parts[2]
        gender = "1" if parts[3] == "male" else "2"
        birthday = parts[4].split("-")
        year, month, day = birthday[0], birthday[1], birthday[2]        
        creation_date = VerticesProcessor.get_unix_timestamp(parts[5])
        return [v_id, f_name, l_name, gender, year, month, day, creation_date]

    @staticmethod
    def process_place(v_id, parts):
        name = parts[1]
        p_type = VerticesProcessor.map_place_type(parts[3])
        return [v_id, name, p_type]

    @staticmethod
    def process_post(v_id, parts):
        ("id,imageFile:STRING,creationDate:INT,locationIP:STRING,"
        "browserUsed:INT,language:STRING,content:STRING,length:INT")
        image = parts[1]
        c_date = VerticesProcessor.get_unix_timestamp(parts[2])
        ip = parts[3]
        browser = VerticesProcessor.map_browser(parts[4])
        language = parts[5]
        content = parts[6]
        length = parts[7]
        return [v_id, image, c_date, ip, browser, language, content, length]

    @staticmethod
    def process_tag(v_id, parts):
        name = parts[1]
        return [v_id, name]

    @staticmethod
    def process_tagclass(v_id, parts):
        name = parts[1]
        return [v_id, name]

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
        date_str = broken[0] + broken[1][3:]
        d_time = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
        return str(int(time.mktime(d_time.timetuple())))
