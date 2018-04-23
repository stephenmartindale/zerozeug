from . import go

import os
import re
import sqlite3

def get_result(filename):
    with open(filename, 'r') as f:
        sgf = f.read()

        player_property = r'\[.*?\s+([0-9a-f]{8})\]'
        match = re.search('PB' + player_property, sgf)
        if (None == match):
            return None
        else:
            black = match.group(1)

        match = re.search('PW' + player_property, sgf)
        if (None == match):
            return None
        else:
            white = match.group(1)

        match = re.search(r'RE\[([BW])(.*?)\]', sgf)
        if (None == match):
            return None
        else:
            victor = go.Stone.Black if ('B' == match.group(1)) else go.Stone.White
            resign = match.group(2).strip().endswith('Resign')

        return {
            'black': black,
            'white': white,
            'victor': victor,
            'resign': resign
        }

def import_self_plays(db_path, sgf_path):
    imported = 0
    failed = 0
    skipped = 0
    ignored = 0
    with sqlite3.connect(db_path) as sql:
        for f in os.listdir(sgf_path):
            id = f[0:32]
            exists = sql.execute("SELECT EXISTS(SELECT id FROM SelfPlay WHERE id=? LIMIT 1)", [id]).fetchone()
            if not (exists[0]):
                result = get_result(os.path.join(sgf_path, f))
                if (None == result):
                    print('Failed to parse SGF file: ' + f)
                    failed += 1
                    continue
                elif (result['black'] != result['white']):
                    ignored += 1
                    continue

                network = result['black']
                network_record = sql.execute('SELECT id FROM Network WHERE id LIKE ?', [network + '%']).fetchone()
                if (None == network_record):
                    failed += 1
                    continue

                sql.execute('INSERT INTO SelfPlay(id, network, victor, resign) VALUES(?, ?, ?, ?)', (id, network_record[0], result['victor'], result['resign']))
                imported += 1

            else:
                skipped += 1

        sql.commit()

    message = '{0} self-play game records imported.'.format(imported)
    if (failed > 0) and (skipped > 0):
        message = message + ' ({0} failed, {1} skipped)'.format(failed, skipped)
    elif (failed > 0):
        message = message + ' ({0} failed)'.format(failed)
    elif (skipped > 0):
        message = message + ' ({0} skipped)'.format(skipped)

    print(message)

    if (ignored > 0):
        print('({0} match game records ignored.)'.format(ignored))
