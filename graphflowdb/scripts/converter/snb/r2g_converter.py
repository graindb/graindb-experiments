import os
import shutil
from .r2g_edges_processor import Relation2GraphEdgesProcessor
from .r2g_vertices_processor import RelationToGraphVerticesProcessor


# Changes:
# 1. comment and post are merged as message
# 2. hasTag is divided into forumHasTag and messageHasTag
# 3. isLocatedIn is divided into messageIsLocatedIn, personIsLocatedIn, forumIsLocatedIn

# message -> v-message: it contains comment and post. need a bool field to indicate if it is a comment
# forum -> v-forum
# organisation -> v-organisation
# person -> v-person
# place -> v-place
# tag -> v-tag
# tagclass -> v-tagclass

# message(m_ps_forumid) -> e-containerOf [forum->post: 1-n]
# message(m_creatorid), person -> e-hasCreator [message->person, n-1]
# person_tag -> e-hasInterest [person->tag: n-n]
# forum_person -> e-hasMember [forum->person: n-n]
# forum(f_moderatorid), person -> e-hasModerator [forum->person: n-1]
# message_tag -> e-messageHasTag [message->tag: n-n]
# forum_tag -> e-forumHasTag [forum->tag: n-n]
# tag(t_tagclassid), tagclass -> e-hasType [tag->tagclass: n-1]
# person(p_placeid), place -> e-personIsLocatedIn [person->place: n-1]
# message(m_m_locationid), place -> e-messageIsLocatedIn [message->place: n-1]
# organisation(o_placeid), place -> e-organisationIsLocatedIn [organisation->place: n-1]
# place(pl_containerplaceid), place -> e-isPartOf [place->place :n-1]
# tagclass(tc_subclassoftagclassid), tagclass -> e-isSubclassOf [tagclass->tagclass: n-1]
# knows -> e-knows [person->person: n-n]
# likes -> e-likes [person->message: n-n]
# message(m_c_replyof), message -> e-replyOf [message->message: n-1]
# person_university -> e-studyAt [person->organisation: n-n]
# person_company -> e-workAt [person->organisation: n-n]

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
        global_map = RelationToGraphVerticesProcessor(self.input_dir,
                                                      self.output_dir, self.post_fix).convert()
        Relation2GraphEdgesProcessor(global_map, self.input_dir,
                                     self.output_dir, self.post_fix).convert()
