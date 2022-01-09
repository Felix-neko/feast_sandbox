import jinja2
import pickle

import pprint

if __name__ == "__main__":
    template_context = pickle.load(open("template_context.pck", 'rb'))

    pprint.pprint(template_context)