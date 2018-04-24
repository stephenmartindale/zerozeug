from . import go

import os
import re
import sqlite3

def parse_result(sgf):
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

def import_self_plays(db_path, sgf_path, report_progress):
    network_counts = {}

    count = 0
    imported = 0
    failed = 0
    ignored = 0
    with open(sgf_path) as f:
        for line in f:
            if (line.startswith('(;')):
                count += 1

                result = parse_result(line)
                if (None == result):
                    failed += 1
                elif (result['black'] != result['white']):
                    ignored += 1
                else:
                    imported += 1

                    network_id = result['black']
                    victor = result['victor']
                    if not (network_id in network_counts):
                        network_counts[network_id] = [1, 0] if (go.Stone.Black == victor) else [0, 1]
                    elif (go.Stone.Black == victor):
                        network_counts[network_id][0] += 1
                    else:
                        network_counts[network_id][1] += 1

                if ((count % 1000) == 0):
                    report_progress(imported, failed, ignored)

    with sqlite3.connect(db_path) as sql:
        for (network_id, victory_counts) in network_counts.items():
            network_record = sql.execute('SELECT id FROM Network WHERE id LIKE ?', [network_id + '%']).fetchone()
            if (None == network_record):
                print("Warning: Failed to resolve full Network identifier: " + network_id)
                continue

            sql.execute('INSERT OR REPLACE INTO SelfPlay(network_id, black_victories, white_victories) VALUES(?, ?, ?)', (network_record[0], victory_counts[0], victory_counts[1]))

        sql.commit()

    return network_counts
