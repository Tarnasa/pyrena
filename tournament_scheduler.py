#!/usr/bin/env python3
"""
Before running this script (The Tournament Scheduler), you must have:
- Setup a postgres server and created the tables a la https://github.com/siggame/ophelia/blob/develop/db/init.sql
- Installed the psycopg2 python3 library (for postgres access)
- Passed in all relevant ENVIRONMENT_VARIABLES below
"""

# pip install psycopg2
import psycopg2
import psycopg2.extras

# Builtin libraries
import base64
import collections
import datetime
import itertools
import json
import logging
import math
import os
import pprint
import random
import shutil
import signal
import string
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import zipfile

# Tournament_scheduler is controlled by these environment variables
GAME_NAME = os.getenv('GAME_NAME', 'Chess')  # Capitalization matters!
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')
REFRESH_SECONDS = int(os.getenv('REFRESH_SECONDS', 30))
N_ELIMINATION = int(os.getenv('N_ELIMINATION', 3))
BEST_OF = int(os.getenv('BEST_OF', '7'))
REUSE_OLD_GAMES = bool(os.getenv('REUSE_OLD_GAMES', True))
OUTPUT_FILE = os.getenv('OUTPUT_FILE', 'tournament.dot')

class Submission(object):
    pass
BYE = Submission()
BYE.id = -1
BYE.name = 'BYE'
BYE.version = -1
BYE.status = 'BYE'
BYE.created_at = None

logging.getLogger().setLevel(logging.INFO)

def main():
    logging.info(f'connecting to database "{DB_NAME}" at {DB_USER}@{DB_HOST}:{DB_PORT}')
    conn = psycopg2.connect(dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            connect_timeout=10,
            cursor_factory=psycopg2.extras.NamedTupleCursor)

    signal.signal(signal.SIGINT, sigint_handler)

    logging.info('Getting latest submissions')
    submissions = get_latest_submissions(conn)

    global nodes
    nodes = list()
    generate_n_elimination_bracket_online(submissions, nodes, N_ELIMINATION)

    try:
        while True:
            update_game_status(conn, [nodes])
            logging.info('Declaring and propogating winners')
            for node in nodes:
                declare_and_propogate_winners(node)
            winner = generate_n_elimination_bracket_online(submissions, nodes, N_ELIMINATION)
            if winner:
                logging.info('Tournament complete')
                logging.info(f'Winner is {winner.winner.name}')
                print_and_save_dot_file()
                break
            create_needed_games(conn, [nodes])
            logging.debug(f'Sleeping {REFRESH_SECONDS}')
            #print_tree([nodes])
            time.sleep(REFRESH_SECONDS)
    except KeyboardInterrupt:
        logging.warning('Caught keyboard interrupt')

def sigint_handler(signal, frame):
    logging.warning('Caught SIGINT')
    print_and_save_dot_file()

def print_and_save_dot_file():
    global nodes
    s = dot_nodes(nodes)
    print(s)
    logging.info(f'Writing dot file to {OUTPUT_FILE}')
    try:
        with open(OUTPUT_FILE, 'w') as f:
            f.write(s)
    except Exception as e:
        logging.warning(traceback.format_exc())

def get_latest_submissions(conn):
    cur = conn.cursor()
    q = '''
SELECT s.id, t.name, s.version, s.status, s.created_at
FROM submissions s
INNER JOIN (
    SELECT team_id, MAX(version) as version
    FROM submissions
    WHERE status != 'failed'
    GROUP BY team_id
) m ON
s.team_id = m.team_id AND
s.version = m.version
INNER JOIN teams t
ON s.team_id = t.id
WHERE t.team_captain_id IS NOT NULL
AND t.is_eligible
AND s.status != 'failed'
    '''
    cur.execute(q)
    return cur.fetchall()

def get_unused_game(conn, left_submission, right_submission, used_game_ids):
    cur = conn.cursor()
    q = '''
SELECT g.id, g.winner_id, g.log_url, g.status
FROM games g
INNER JOIN games_submissions gs1
ON g.id = gs1.game_id
INNER JOIN games_submissions gs2
ON g.id = gs2.game_id
WHERE gs1.submission_id = %s
AND gs2.submission_id = %s
AND g.status = 'finished'
AND g.id NOT IN %s
ORDER BY g.id DESC
LIMIT 1
    '''
    cur.execute(q, (left_submission.id, right_submission.id, (-1, *used_game_ids)))
    return cur.fetchall()

class Node:
    def __init__(self):
        self.submissions = list()
        self.feeders = list()
        self.inverted_feeders = list()
        self.games = list()
        self.winner = None
        self.loser = None

        self.winner_child = None
        self.loser_child = None

        self.left_submission = None
        self.right_submission = None
        self.left_feeder = None
        self.right_feeder = None
        self.left_inverted = False
        self.right_inverted = False

def generate_initial_pairing(submissions):
    width = 2**int(math.ceil(math.log2(len(submissions))) - 1)
    shuffled_submissions = list(submissions)
    random.shuffle(shuffled_submissions)
    shuffled_submissions += [BYE] * (2*width - len(shuffled_submissions))
    nodes = [Node() for _ in range(width)]
    i = 0
    for node in nodes:
        node.submissions.append(shuffled_submissions[i])
        i += 1
    for node in nodes:
        node.submissions.append(shuffled_submissions[i])
        i += 1
    return nodes

def generate_single_elimination_bracket(submissions):
    width = 2**int(math.ceil(math.log2(len(submissions))) - 1)
    print(math.ceil(math.log2(len(submissions))))
    level = generate_initial_pairing(submissions)
    levels = [level]
    lower_level = level
    while width > 1:
        width //= 2
        level = list()
        for i in range(width):
            node = Node()
            node.feeders.append(lower_level[i*2])
            node.feeders.append(lower_level[i*2+1])
            level.append(node)
        levels.append(level)
        lower_level = level
    return levels

# Must be called continuosly as winners are updated
# When the tournament is finished, returns the winner node
# Otherwise returns false
def generate_n_elimination_bracket_online(submissions, nodes, max_losses):
    wins = collections.defaultdict(lambda: 0)
    losses = collections.defaultdict(lambda: 0)
    if not nodes:
        nodes.extend(generate_initial_pairing(submissions))
    for node in nodes:
        if node.loser:
            losses[node.loser] += 1
        if node.winner:
            wins[node.winner] += 1
    available = list()
    pending_matches = False
    for node in nodes:
        if node.winner and not node.winner_child:
            available.append((node, node.winner))
        if node.loser and not node.loser_child:
            if losses[node.loser] < max_losses:
                available.append((node, node.loser))
        if not node.winner:
            pending_matches = True
    # Finished!
    if not pending_matches and len(available) == 1:
        return available[0][0]
    if not pending_matches and len(available) == 0:
        logging.error('No matches, and no available players!')
        return nodes[-1]
    print(pending_matches)
    print(len(available))
    # Try to balance the matches so that teams progress at an even rate through the bracket
    available_by_score = collections.defaultdict(lambda: list())
    for node, who in available:
        available_by_score[(losses[who], wins[who])].append((node, who))
    available_by_losses = collections.defaultdict(lambda: list())
    for node, who in available:
        available_by_losses[losses[who]].append((node, who))
    groups = list()
    groups.append(available_by_score)
    groups.append(available_by_losses)
    groups.append({0: list(sorted(available, key=lambda p: -losses[p[1]]))})
    for group in groups:
        print('group', len(group))
        for k, node_sources in group.items():
            print('node sources', k, len(node_sources))
            for pair in pairwise(node_sources):
                new = Node()
                for node, who in pair:
                    if who is node.winner:
                        new.feeders.append(node)
                        node.winner_child = new
                    elif who is node.loser:
                        new.inverted_feeders.append(node)
                        node.loser_child = new
                    else:
                        logging.error('bad who')
                nodes.append(new)
                pending_matches = True
        if pending_matches:
            break
    return False

def pairwise(collection):
    return zip(*([iter(collection)] * 2))

def get_node_label(node):
    names = list()
    for submission in node.submissions:
        names.append(f'{submission.name}_{submission.id}')
    names += ['-'] * (2 - len(names))
    label = f'{names[0]} vs {names[1]}'
    if node.games:
        left_wins = sum(1 for g in node.games if g.winner_id == node.submissions[0].id)
        right_wins = sum(1 for g in node.games if g.winner_id == node.submissions[1].id)
        label = f'{names[0]}({left_wins}/{BEST_OF}) vs {names[1]}({right_wins}/{BEST_OF})'
        if node.winner:
            representative_games = [g for g in node.games if g.winner_id == node.winner.id]
            if representative_games:
                label += r'\n' + representative_games[0].log_url
    return label

def _print_tree(node, depth):
    global _printed
    if node is None or node in _printed:
        return
    _printed.add(node)
    feeders = list(node.feeders) + list(node.inverted_feeders)
    if len(feeders) >= 1:
        _print_tree(feeders[0], depth - 1)
    line = ' '*10*depth + get_node_label(node)
    print(line)
    if len(feeders) >= 2:
        _print_tree(feeders[1], depth - 1)

def print_tree(levels):
    global _printed
    _printed = set()
    print('-'*40)
    _print_tree(levels[-1][0], len(levels) - 1)
    print('-'*40)

def _dot_tree(node):
    global _printed
    if node is None:
        return
    if node in _printed:
        return
    _printed.add(node)
    for feeder in node.feeders:
        print(f'  {id(feeder)} -> {id(node)} [style=solid];')
    for feeder in node.inverted_feeders:
        print(f'  {id(feeder)} -> {id(node)} [style=dotted];')
    feeders = list(node.feeders) + list(node.inverted_feeders)
    if feeders:
        _dot_tree(feeders[0])
    label = get_node_label(node)
    print(f'  {id(node)} [label="{label}"];')
    if len(feeders) > 1:
        _dot_tree(feeders[1])

def dot_tree(node):
    global _printed
    _printed = set()
    print('digraph bracket {')
    print('  rankdir=LR')
    _dot_tree(node)
    print('}')

def dot_nodes(nodes):
    s = ''
    s += 'digraph bracket {\n'
    s += '  rankdir=LR\n'
    for node in nodes:
        for feeder in node.feeders:
            s += f'  {id(feeder)} -> {id(node)} [style=solid];\n'
        for feeder in node.inverted_feeders:
            s += f'  {id(feeder)} -> {id(node)} [style=dotted];\n'
        label = get_node_label(node)
        s += f'  {id(node)} [label="{label}"];\n'
    s += '}\n'
    return s

def get_games(conn, game_ids):
    cur = conn.cursor()
    q = '''
SELECT id, status, winner_id, log_url
FROM games
WHERE id IN %s;
    '''
    cur.execute(q, (tuple(game_ids),))
    conn.commit()
    return cur.fetchall()

def update_game_status(conn, levels):
    game_ids = list()
    for level in levels:
        for node in level:
            for game in node.games:
                if game.status != 'finished':
                    game_ids.append(game.id)
    if not game_ids:
        return
    logging.info(f'Retrieving status for {len(game_ids)} games')
    games = get_games(conn, game_ids)
    games_by_id = {g.id: g for g in games}
    for level in levels:
        for node in level:
            for i, game in enumerate(node.games):
                if game.id in games_by_id:
                    node.games[i] = games_by_id[game.id]

def propogate_winners(node):
    if node.feeders or node.inverted_feeders:
        node.submissions = list()
    for feeder in node.feeders:
        if feeder.winner:
            node.submissions.append(feeder.winner)
    for feeder in node.inverted_feeders:
        if feeder.loser:
            node.submissions.append(feeder.loser)

def declare_and_propogate_winners(node):
    if node is None:
        return
    if node.winner and node.loser:
        return
    for feeder in node.feeders:
        declare_and_propogate_winners(feeder)
    propogate_winners(node)
    # Handle byes
    if len(node.submissions) == 2:
        if node.submissions[0] is BYE and node.submissions[1]:
            node.winner = node.submissions[1]
            node.loser = BYE
        if node.submissions[1] is BYE and node.submissions[0]:
            node.winner = node.submissions[0]
            node.loser = BYE
        if node.submissions[0] is BYE and node.submissions[1] is BYE:
            node.winner = node.loser = BYE
    # Handle playing yourself
    if len(node.submissions) == 2:
        if node.submissions[0] == node.submissions[1]:
            node.winner = node.submissions[0]
            node.loser = node.submissions[1]
    # Declare match winners from games played
    if not node.winner:
        winners = collections.Counter(g.winner_id for g in node.games if g.winner_id)
        for winner_id, wins in winners.items():
            if wins > (BEST_OF // 2) and winner_id is not None:
                for pair in zip(node.submissions, reversed(node.submissions)):
                    if pair[0].id == winner_id:
                        node.winner = pair[0]
                        node.loser = pair[1]
                        break
                else:
                    raise Exception(f'Winner {winner_id} was not a member of this node')

def create_or_reuse_game(conn, left, right):
    if REUSE_OLD_GAMES:
        global nodes
        used_game_ids = [game.id for node in nodes for game in node.games]
        games = get_unused_game(conn, left, right, used_game_ids)
        if games:
            game = games[0]
            logging.info(f'Re-used old match {game.id} for {left.name}({left.id}) vs. {right.name}({right.id})')
            return game
    logging.info(f'Enqueueing match for {left.name}({left.id}) vs. {right.name}({right.id})')
    return create_queued_game(conn, left, right)

def create_queued_game(conn, left_submission, right_submission):
    cur = conn.cursor()
    q = '''
INSERT INTO games (
 status
) VALUES (
'queued'
) RETURNING id, status, winner_id;
    '''
    cur.execute(q)
    game = cur.fetchone()
    q = '''
INSERT INTO games_submissions (
 game_id,
 submission_id
) VALUES
(%s, %s),
(%s, %s)
    '''
    cur.execute(q, (game.id, left_submission.id, game.id, right_submission.id))
    cur.close()
    conn.commit()
    return game

def create_needed_games(conn, levels):
    for level in levels:
        for node in level:
            if not node.winner and len(node.submissions) == 2:
                if BYE in node.submissions:
                    continue
                finished_or_queued_games = [g for g in node.games if g.status in ['finished', 'queued', 'playing']]
                for i in range(len(finished_or_queued_games), BEST_OF):
                    left, right = node.submissions
                    # Try to mitigate first-turn advantage by switching player order
                    if i % 2:
                        left, right = right, left
                    game = create_or_reuse_game(conn, left, right)
                    node.games.append(game)

if __name__ == '__main__':
    main()
