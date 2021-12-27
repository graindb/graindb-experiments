import os
import time
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class JobRelationToGraphVerticesProcessor:
    vertex_files = [
        "aka_name",
        "aka_title",
        "char_name",
        "comp_cast_type",
        "company_name",
        "company_type",
        "info_type",
        "keyword",
        "kind_type",
        "link_type",
        "name",
        "person_info",
        "role_type",
        "title"
    ]

    file_headers = [
        # aka_name
        "id:INT,person_id:INT,name:STRING,imdb_index:STRING,name_pcode_cf:STRING,name_pcode_nf:STRING,surname_pcode:STRING,md5sum:STRING",
        # aka_title
        "id:INT,movie_id:INT,title:STRING,imdb_index:STRING,kind_id:INT,production_year:INT,phonetic_code:STRING,episode_of_id:INT,season_nr:INT,episode_nr:INT,note:STRING,md5sum:STRING",
        # char_name
        "id:INT,name:STRING,imdb_index:STRING,imdb_id:INT,name_pcode_nf:STRING,surname_pcode:STRING,md5sum:STRING",
        # comp_cast_type
        "id:INT,kind:STRING",
        # company_name
        "id:INT,name:STRING,country_code:STRING,imdb_id:INT,name_pcode_nf:STRING,name_pcode_sf:STRING,md5sum:STRINGT",
        # company_type
        "id:INT,kind:STRING",
        # info_type
        "id:INT,info:STRING",
        # keyword
        "id:INT,keyword:STRING,phonetic_code:STRING",
        # kind_type
        "id:INT,kind:STRING",
        # link_type
        "id:INT,link:STRING",
        # name
        "id:INT,name:STRING,imdb_index:STRING,imdb_id:INT,gender:STRING,name_pcode_cf:STRING,name_pcode_nf:STRING,surname_pcode:STRING,md5sum:STRING",
        # person_info
        "id:INT,person_id:INT,info_type_id:INT,info:STRING,note:STRING",
        # role_type
        "id:INT,role:STRING",
        # title
        "id:INT,title:STRING,imdb_index:STRING,kind_id:INT,production_year:INT,imdb_id:INT,phonetic_code:STRING,episode_of_id:INT,season_nr:INT,episode_nr:INT,series_years:STRING,md5sum:STRING"
    ]

    @staticmethod
    def line_processors():
        return {}

    def __init__(self, in_dir, out_dir, post_fix="_0_0.csv"):
        self.count = 0
        self.global_map = dict()
        self.input_dir = in_dir
        self.output_dir = out_dir
        self.post_fix = post_fix

    def convert(self):
        files = JobRelationToGraphVerticesProcessor.vertex_files
        headers = JobRelationToGraphVerticesProcessor.file_headers
        processors = JobRelationToGraphVerticesProcessor.line_processors()
        for i in range(len(files)):
            print("> Processing vertices", files[i], "...")
            processor = processors[files[i]] if files[i] in processors else \
                JobRelationToGraphVerticesProcessor.process_default
            self.process_file(files[i], headers[i], processor)
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
    def process_default(v_id, parts):
        result = [v_id, v_id]
        result.extend(parts)
        return result

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
