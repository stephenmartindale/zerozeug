from . import go

import os
import re

def get_result(filename):
    with open(filename, 'r') as f:
        sgf = f.read()
        match = re.search(r'RE\[([BW])(.*?)\]', sgf)
        if (None == match):
            return None
        else:
            colour = go.Stone.Black if ('B' == match.group(1)) else go.Stone.White
            resign = match.group(2).strip().endswith('Resign')
            return (colour, resign, match.group(0))
