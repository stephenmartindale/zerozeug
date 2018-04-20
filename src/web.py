from . import go

import os
from datetime import datetime
import sqlite3
import urllib.request, urllib.parse
from bs4 import BeautifulSoup


uri = 'http://zero.sjeng.org/'


def match_id(href):
    return href[(href.rfind('/') + 1):]

def match_uri(id):
    return urllib.parse.urljoin(uri, '/match-games/' + id)

def network_id(href):
    return href[(href.rfind('/') + 1):href.rfind('.gz')]

def sgf_uri(hash):
    return urllib.parse.urljoin(uri, '/viewmatch/' + hash + '.sgf')


# Fetch the index of matches from the Leela Zero training-graph page
def fetch_index():
    # Fetch the Leela Zero Index Page
    with urllib.request.urlopen(uri) as r:
        leelaIndex = BeautifulSoup(r.read(), "html5lib")

    # Table of Networks
    networkTable = leelaIndex.select('table.networks-table')[0]
    networks = {}
    for tr in networkTable('tr'):
        cells = tr('td')
        if (len(cells) != 6):
            continue

        a = cells[2]('a')
        id = network_id(a[0]['href'])

        networks[id] = {
            'id': id,
            'promoted': True
        }

    # Table of Circa 100 Recent Matches
    matchTable = leelaIndex.select('table.matches-table')[0]
    matches = {}
    for tr in matchTable('tr'):
        cells = tr('td')
        if (len(cells) != 5):
            continue

        sprt = cells[4].string.strip()
        if 'fail' == sprt:
            result = False
        elif 'PASS' == sprt:
            result = True
        else:
            result = None

        try:
            startDate = datetime.strptime(cells[0].string.strip(), '%Y-%m-%d %H:%M')
        except:
            continue

        a = cells[1]('a')
        if (len(a) != 3):
            continue
        elif ('VS' != a[1].string.strip()):
            continue

        challenger = network_id(a[0]['href'])
        id = match_id(a[1]['href'])
        defender = network_id(a[2]['href'])

        if not (challenger in networks):
            networks[challenger] = {
                'id': challenger,
                'promoted': False
            }

        if not (defender in networks):
            networks[defender] = {
                'id': challenger,
                'promoted': False
            }

        matches[id] = {
            'id': id,
            'start_date': startDate,
            'result': result,
            'challenger': network_id(a[0]['href']),
            'defender': network_id(a[2]['href'])
        }

    return networks, matches


# Fetch the index of match-games for a given match
def fetch_match_index(match):
    match_id = match['id']
    with urllib.request.urlopen(match_uri(match_id)) as r:
        matchIndex = BeautifulSoup(r.read(), "html5lib")
        matchTable = matchIndex.select('table#sort')[0]

        challenger = match['challenger']
        defender = match['defender']

        games = {}
        for row in matchTable('tr'):
            cells = row('td')
            if (len(cells) != 6):
                continue

            outcome = cells[3].string.strip()
            if (outcome.startswith('B')):
                victor = go.Stone.Black
            elif (outcome.startswith('W')):
                victor = go.Stone.White
            else:
                continue

            winner = cells[2].string.strip()
            if (challenger.startswith(winner)):
                black = challenger if (victor == go.Stone.Black) else defender
                white = challenger if (victor == go.Stone.White) else defender
            elif (defender.startswith(winner)):
                black = defender if (victor == go.Stone.Black) else challenger
                white = defender if (victor == go.Stone.White) else challenger
            else:
                continue

            id = cells[1].string.strip()

            games[id] = {
                'id': id,
                'match_id': match_id,
                'client': int(cells[0].string.strip()),
                'black': black,
                'white': white,
                'moves': int(cells[4].string.strip()),
                'victor': victor,
                'resign': outcome.endswith('Resign')
            }

        return games


# Fetch all available data and store it in Sqlite
def fetch_database(db_path):
    try:
        if not os.path.isfile(db_path):
            # Initialise the Database on first use...
            print('Creating new Sqlite Database ...')

            if not os.path.exists(os.path.dirname(db_path)):
                os.makedirs(os.path.dirname(db_path))

            sql = sqlite3.connect(db_path)

            sql.execute('CREATE TABLE Stone(stone int primary key not null,'
                                           'description char(5) not null)')
            sql.execute('INSERT INTO Stone(stone, description) VALUES (1, "black"),'
                                                                     '(2, "white")')

            sql.execute('CREATE TABLE Network(id char(64) primary key not null,'
                                             'promoted boolean not null default false)')

            sql.execute('CREATE TABLE Match(id char(24) primary key not null,'
                                           'start_date datetime not null,'
                                           'result boolean null,'
                                           'challenger char(64) not null,'
                                           'defender char(64) not null)')

            sql.execute('CREATE TABLE Game(id char(64) primary key not null,'
                                          'match_id char(24) not null references [Match](id) on delete cascade,'
                                          'client int not null,'
                                          'black char(64) not null references [Network](id) on delete no action,'
                                          'white char(64) not null references [Network](id) on delete no action,'
                                          'moves int not null,'
                                          'victor int not null references [Stone](stone) on delete no action,'
                                          'resign bool not null)')

        else:
            # Connect to the Database and assume that the Schema is valid
            print('Connecting to Sqlite Database ...')
            sql = sqlite3.connect(db_path)

        # Fetch the index of matches from the Leela Zero training-graph page
        print('Fetching networks and matches ...')
        (networks, matches) = fetch_index()

        # Populate or Update the table of Networks
        print('Storing Networks ...')
        for id in networks:
            exists = sql.execute("SELECT EXISTS(SELECT id FROM Network WHERE id=? LIMIT 1)", [id]).fetchone()
            if not (exists[0]):
                sql.execute('INSERT INTO Network(id, promoted) VALUES(?, ?)', (id, networks[id]['promoted']))
            else:
                sql.execute('UPDATE Network SET promoted=? WHERE id=?', (networks[id]['promoted'], id))

        # Populate or Update the table of Matches
        print('Storing Matches ...')
        for id in matches:
            exists = sql.execute("SELECT EXISTS(SELECT id FROM Match WHERE id=? LIMIT 1)", [id]).fetchone()
            if not (exists[0]):
                sql.execute('INSERT INTO Match(id, start_date, result, challenger, defender) VALUES(?, ?, ?, ?, ?)', (id, matches[id]['start_date'], matches[id]['result'], matches[id]['challenger'], matches[id]['defender']))
            else:
                sql.execute('UPDATE Match SET result=? WHERE id=?', (matches[id]['result'], id))

        # Populate or Update the table of Games
        print('Fetching & Storing Game Results ...')
        for match_id in matches:
            games = fetch_match_index(matches[match_id])
            matches[match_id]['games'] = games
            for game_id in games:
                exists = sql.execute("SELECT EXISTS(SELECT id FROM Game WHERE id=? LIMIT 1)", [game_id]).fetchone()
                if not (exists[0]):
                    sql.execute('INSERT INTO Game(id, match_id, client, black, white, moves, victor, resign) VALUES(?, ?, ?, ?, ?, ?, ?, ?)', (game_id, match_id, games[game_id]['client'], games[game_id]['black'], games[game_id]['white'], games[game_id]['moves'], games[game_id]['victor'], games[game_id]['resign']))

        # Commit the Transaction
        sql.commit()
        print('Transaction committed successfully.')

    finally:
        # Disconnect
        sql.close()
        print('done.')
