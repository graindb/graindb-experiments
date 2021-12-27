import logging
from .g2g_converter import GraphToGraphConverter
from .r2g_converter import RelationToGraphConverter
from .r2r_converter import RelationToRelationConverter

logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Convert SNB to Graphflow format.')
    parser.add_argument('type', help='R2G(RelationToGraph), R2R(RelationToRelation), G2G(GraphToGraph).')
    parser.add_argument('input_dir', help='Path to the dir that contains SNB format graph.')
    parser.add_argument('output_dir', help='Output dir path for Graphflow format graph.')
    args = parser.parse_args()
    convert_type = args.type.upper()
    if convert_type == 'R2G':
        RelationToGraphConverter(args.input_dir, args.output_dir).convert()
    elif convert_type == 'G2G':
        GraphToGraphConverter(args.input_dir, args.output_dir).convert()
    elif convert_type == 'R2R':
        RelationToRelationConverter(args.input_dir, args.output_dir).convert()
    else:
        print("Not supported convert type %s", args.type)