#!/usr/bin/env python3
"""
Before running this script (The Pyrena), you must have:
- Setup a postgres server and created the tables a la https://github.com/siggame/ophelia/blob/develop/db/init.sql
- Setup a siggame gameserver https://github.com/siggame/cerveau
- Setup a https://github.com/tarnasa/droopy server (for uploading logs)
- Installed the psycopg2 python3 library (for postgres access)
- Installed docker on this machine
- Placed per-language Dockerfiles in a directory like:
    per_language_dockerfiles/py/Dockerfile
    per_language_dockerfiles/cpp/Dockerfile
    ...
  (This can be done easily by cloning siggame/joueur)
- Passed in all relevant ENVIRONMENT_VARIABLES below
- Run this script as a user with docker permissions
"""

# pip install psycopg2
import psycopg2
import psycopg2.extras

# Builtin libraries
import base64
import datetime
import json
import logging
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

# Pyrena is controlled by these environment variables
GAME_NAME = os.getenv('GAME_NAME', 'Chess')  # Capitalization matters!
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')
GAMESERVER_HOST = os.getenv('GAMESERVER_HOST', 'localhost')
GAMESERVER_TCPPORT = os.getenv('GAMESERVER_TCPPORT', '3000')
GAMESERVER_WEBPORT = os.getenv('GAMESERVER_WEBPORT', '3080')
DROOPY_URL = os.getenv('DROOPY_URL', 'http://localhost:8000/') # Note trailing slash
DROOPY_CREDS = os.getenv('DROOPY_CREDS', 'USER:PASS')  # Leave empty for no creds
DOCKERFILE_PATH = os.getenv('DOCKERFILE_PATH', '/per_language_dockerfiles')
RUN_FOREVER = os.getenv('RUN_FOREVER', False)
LOGFILE_PATH = os.getenv('LOGFILE_PATH', '/tmp/pyrena_logfiles')
SUBMISSION_CACHE_PATH = os.getenv('SUBMISSION_CACHE_PATH', '/tmp/submission_cache')
LOOKBACK_SECONDS = int(os.getenv('LOOKBACK_SECONDS', 60*60*1))
CONTAINER_CPU = os.getenv('CONTAINER_CPU', '0.5')
CONTAINER_RAM = os.getenv('CONTAINER_RAM', '1g')
MATCH_TIMEOUT = int(os.getenv('MATCH_TIMEOUT', 60*5))
DOCKER_BIN = os.getenv('DOCKER_BIN', '/usr/bin/docker')

KNOWN_LANGUAGE_EXTENSIONS = 'py cpp cs lua java js ts'.split()

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

    while True:
        try:
            game_id = None
            logging.info('Getting latest submissions')
            latest_submissions = get_latest_submissions(conn)
            all_submissions = get_all_submissions(conn)
            game_id, pair = grab_queued_game(conn, all_submissions)
            if game_id:
                logging.info(f'Grabbed queued game {game_id}')
            else:
                logging.info('Getting recent games')
                games = get_recent_games(conn, datetime.timedelta(seconds=LOOKBACK_SECONDS))
                logging.info('Generating pairing')
                pair = generate_nonrecent_pairing(latest_submissions, games)
                logging.info('Inserting new game')
                game_id = insert_new_game_row(conn, pair)
            logging.info(f'Playing match: {pair[0].name}({pair[0].id}) v {pair[1].name}({pair[1].id})')
            for submission in pair:
                maybe_download_submission(conn, submission.id)
                try:
                    maybe_unzip_submission(submission.id)
                    verify_submission_contents(submission.id)
                except Exception as e:
                    report_prebuild_failure(conn, submission.id, str(e))
                    raise e
                replace_dockerfile(submission.id)
                maybe_build_submission_container(conn, submission.id)
            logging.info('Setting up room')
            session = session_name(game_id, pair)
            password = setup_room(game_id, pair)
            logging.info(f'Set up game {session} with password {password}')
            processes = []
            stdouts = []
            for i, submission in enumerate(pair):
                logging.info(f'Starting up docker container for {submission.name}({submission.id})')
                p, stdout = start_and_connect_client(session, password, submission, i)
                processes.append(p)
                stdouts.append(stdout)
            logging.info('Waiting for match to finish')
            wait_for_clients_to_finish(pair, processes, stdouts)
            logging.info('Match is over')
            kill_remaining_clients(pair, processes, stdouts)
            for submission in pair:
                stdout_path = match_stdout_path(submission, session)
                droopy_filename = stdout_path.split('/')[-1]
                try:
                    droopy_url = upload_file_to_droopy(stdout_path, droopy_filename)
                    logging.info(f'Adding output_url to game_submissions table for {submission.id}')
                    update_game_submission_logs(conn, droopy_url, game_id, submission.id)
                except Exception as e:
                    logging.warning(traceback.format_exc())
            match_status = wait_for_gameserver_gamelog(session)
            logging.info('Updating game table with match results')
            winner_name = None
            for client_info in match_status['clients']:
                if client_info['won']:
                    winner_name = client_info['name']
                    win_reason = client_info['reason']
                if client_info['lost']:
                    lose_reason = client_info['reason']
            winner_id = next(s.id for s in pair if s.name == winner_name)
            gamelog_name = match_status['gamelogFilename']
            local_gamelog_path = download_gamelog(gamelog_name)
            droopy_gamelog_url = upload_file_to_droopy(local_gamelog_path, gamelog_name)
            update_game_succeeded(conn, win_reason, lose_reason, winner_id, droopy_gamelog_url, game_id)
        except Exception as e:
            logging.warning(traceback.format_exc())
            if game_id is not None:
                logging.info(f'Failing current in-progress match {game_id}')
                update_game_failed(conn, game_id, 'Arena failed to run game')
            conn.rollback()
            time.sleep(15 + random.randint(1, 5))
        except KeyboardInterrupt:
            logging.warning('Caught keyboard interrupt')
            if game_id is not None:
                logging.warning(f'Failing current in-progress match {game_id}')
                update_game_failed(conn, game_id, 'Cancelled by admin')
            logging.info('closing database connection')
            conn.close()
            sys.exit(0)
        if not RUN_FOREVER:
            break
    # END WHILE TRUE
    logging.info('closing database connection')
    conn.close()

def sigint_handler(signal, frame):
    global RUN_FOREVER
    logging.warning('Caught SIGINT')
    if RUN_FOREVER:
        logging.warning('Waiting for current game to complete')
        logging.warning('Press Control-C again to forcibly stop')
        RUN_FOREVER = False
    else:
        raise KeyboardInterrupt

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
    conn.commit()
    return cur.fetchall()

def get_all_submissions(conn):
    cur = conn.cursor()
    q = '''
SELECT s.id, t.name, s.version, s.status, s.created_at
FROM submissions s
INNER JOIN teams t
ON s.team_id = t.id
    '''
    cur.execute(q)
    conn.commit()
    return cur.fetchall()

def get_recent_games(conn, time_backward):
    cur = conn.cursor()
    q = '''
SELECT
  g.id,
  g.status,
  string_agg(gs.submission_id::text, ',') as submission_ids
FROM games g
INNER JOIN games_submissions gs
ON g.id = gs.game_id
WHERE g.created_at > (current_timestamp - %s)
GROUP BY g.id, g.status;
    '''
    cur.execute(q, (time_backward,))
    conn.commit()
    return cur.fetchall()

def grab_queued_game(conn, submissions):
    cur = conn.cursor()
    q = '''
UPDATE games g
SET status = 'playing'
WHERE id = (
  SELECT id
  FROM games
  WHERE status = 'queued'
  ORDER BY id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
RETURNING g.id;
    '''
    cur.execute(q)
    result = cur.fetchone()
    conn.commit()
    if not result:
        return None, None
    q = '''
SELECT gs.submission_id
FROM games_submissions gs
WHERE gs.game_id = %s
    '''
    cur.execute(q, (result.id,))
    conn.commit()
    submission_ids = [s.submission_id for s in cur.fetchall()]
    pair = tuple([s for s in submissions if s.id in submission_ids])
    return result.id, pair

def generate_nonrecent_pairing(submissions, games):
    pairs = list()
    for g in games:
        if g.status == 'queued':
            continue
        submission_ids = map(int, g.submission_ids.split(','))
        pairs.append(tuple(sorted(submission_ids)))
    tries = 200
    pair = generate_pairing(submissions)
    while pair in pairs:
        pair = generate_pairing(submissions)
        tries -= 1
        if tries <= 0:
            raise Exception('Unable to generate non-recent pairing')
    id_to_submissions = {s.id: s for s in submissions}
    return tuple(id_to_submissions[id_] for id_ in pair)

def generate_pairing(submissions):
    ids = [s.id for s in submissions]
    if len(ids) < 2:
        raise Exception(f'Not enough submissions {len(ids)}')
    a = random.choice(ids)
    b = random.choice(ids)
    while b == a:
        b = random.choice(ids)
    return tuple(sorted([a, b]))
    
def insert_new_game_row(conn, pair):
    cur = conn.cursor()
    q = '''
INSERT INTO games (
 status
) VALUES (
'playing'
) RETURNING id;
    '''
    cur.execute(q)
    game_id, = cur.fetchone()
    q = '''
INSERT INTO games_submissions (
 game_id,
 submission_id
) VALUES
(%s, %s),
(%s, %s)
    '''
    cur.execute(q, (game_id, pair[0].id, game_id, pair[1].id))
    cur.close()
    conn.commit()
    return game_id

def submission_filename(submission_id):
    return os.path.join(SUBMISSION_CACHE_PATH, f'submission_{submission_id}.zip')

def unzipped_submission_folder(submission_id):
    return os.path.join(SUBMISSION_CACHE_PATH, f'submission_{submission_id}')

def maybe_download_submission(conn, submission_id):
    filename = submission_filename(submission_id)
    if os.path.exists(filename) and os.path.getsize(filename) > 1024:
        logging.info(f'submission data cached in {filename}')
        return 
    logging.info(f'downloading submission data to {filename}')
    download_submission(conn, submission_id)

def download_submission(conn, submission_id):
    cur = conn.cursor()
    q = '''
SELECT data
FROM submissions
WHERE id = %s
    '''
    cur.execute(q, (submission_id,))
    conn.commit()
    submission_data = cur.fetchone()
    filename = submission_filename(submission_id)
    os.makedirs(SUBMISSION_CACHE_PATH, exist_ok=True)
    with open(filename, 'wb') as f:
        f.write(submission_data.data)

def maybe_unzip_submission(submission_id):
    unzipped_folder = unzipped_submission_folder(submission_id)
    if os.path.isdir(unzipped_folder):
        logging.info(f'submission already unzipped to {unzipped_folder}')
        return
    filename = submission_filename(submission_id)
    logging.info(f'Unzipping {filename} to {unzipped_folder}')
    zf = zipfile.ZipFile(filename, 'r')
    zf.extractall(unzipped_folder)
    zf.close()

def submission_joueur_folder(submission_id):
    unzipped_folder = unzipped_submission_folder(submission_id)
    joueur_path = None
    dirpath, dirnames, filenames = next(os.walk(unzipped_folder))
    expected_prefix = 'Joueur.'
    for dirname in dirnames:
        if dirname.startswith(expected_prefix):
            if dirname[len(expected_prefix):] not in KNOWN_LANGUAGE_EXTENSIONS:
                raise Exception(f'Submission {submission_id} using unkown language: {dirname}')
            joueur_path = os.path.join(dirpath, dirname)
            break
    else:
        raise Exception(f'Submission {unzipped_folder} does not unzip to top-level Joueur.xx')
    return joueur_path

def verify_submission_contents(submission_id):
    joueur_path = submission_joueur_folder(submission_id)
    dirpath, dirnames, filenames = next(os.walk(joueur_path))
    if 'Makefile' not in filenames and 'makefile' not in filenames:
        raise Exception(f'Submission {unzipped_folder} does not have "Makefile"')
    if 'run' not in filenames:
        raise Exception(f'Submission {unzipped_folder} does not have "run" file')

def report_prebuild_failure(conn, submission_id, error_message):
    filename = f'prebuild_failure_{submission_id}'
    path = os.path.join(LOGFILE_PATH, filename)
    with open(path, 'w') as f:
        f.write(error_message)
    url = upload_file_to_droopy(path, filename)
    report_build_status(conn, submission_id, 'failed', url)

def replace_dockerfile(submission_id):
    joueur_path = submission_joueur_folder(submission_id)
    submission_dockerfile_path = os.path.join(joueur_path, 'Dockerfile')
    language = joueur_path.split('.')[-1]
    safe_dockerfile_path = os.path.join(DOCKERFILE_PATH, language, 'Dockerfile')
    if not os.path.isfile(safe_dockerfile_path):
        raise Exception(f'Dockerfile not found at {safe_dockerfile_path}')
    logging.info(f'Copying dockerfile from {safe_dockerfile_path} to {submission_dockerfile_path}')
    shutil.copy2(safe_dockerfile_path, submission_dockerfile_path)
    if not os.path.isfile(submission_dockerfile_path):
        raise Exception(f'Failed to copy dockerfile to {submission_dockerfile_path}')

def report_build_status(conn, submission_id, status, logfile_url):
    cur = conn.cursor()
    q = '''
UPDATE submissions
SET
  status = %s,
  log_url = %s
WHERE id = %s
    '''
    cur.execute(q, (status, logfile_url, submission_id))
    conn.commit()

def submission_docker_tag(submission_id):
    return f'submission_{submission_id}'

def buildlog_filename(submission_id):
    return os.path.join(LOGFILE_PATH, f'dockerbuild_{submission_id}')

def maybe_build_submission_container(conn, submission_id):
    tag = submission_docker_tag(submission_id)
    if subprocess.check_output([DOCKER_BIN, 'images', '-q', tag]):
        logging.info(f'{tag} already built')
        return
    logging.info(f'Building docker image for {submission_id}')
    joueur_path = submission_joueur_folder(submission_id)
    os.makedirs(LOGFILE_PATH, exist_ok=True)
    logfile_path = buildlog_filename(submission_id)
    logging.info(f'Writing build output to {logfile_path}')
    with open(logfile_path, 'w', buffering=1) as logfile:
        args = [DOCKER_BIN, 'build', joueur_path, '-t', tag]
        logging.info('Running: ' + ' '.join(args))
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout:
            printable_line = line.decode('utf-8')
            sys.stdout.write(printable_line)
            logfile.write(printable_line)
    url = upload_file_to_droopy(logfile_path, f'dockerbuild_{submission_id}')
    if not subprocess.check_output([DOCKER_BIN, 'images', '-q', tag]):
        report_build_status(conn, submission_id, 'failed', url)
        raise Exception(f'Failed to build {submission_id}')
    report_build_status(conn, submission_id, 'finished', url)

def session_name(game_id, pair):
    return f'arena_{game_id}_{pair[0].id}v{pair[1].id}'

def generate_password():
    return ''.join(random.choice(string.ascii_letters) for _ in range(16))

# Returns password
def setup_room(game_id, pair):
    endpoint = f'http://{GAMESERVER_HOST}:{GAMESERVER_WEBPORT}/setup'
    password = generate_password()
    body = json.dumps({
            'gameName': GAME_NAME,
            'session': session_name(game_id, pair),
            'password': password,
            'gameSettings': {
                'playerNames': [
                    pair[0].name,
                    pair[1].name,
                    ]
                },
            }).encode('utf-8')
    request = urllib.request.Request(endpoint, data=body, method='POST', headers={'Content-type': 'application/json'})
    try:
        urllib.request.urlopen(request)
    except urllib.error.HTTPError as e:
        logging.info(f'POST body: {body}')
        raise Exception(e.read().decode('utf-8')) from e
    return password

def docker_name(session, submission):
    return f'{submission.id}_for_{session}'

def match_stdout_path(submission, session):
    return os.path.join(LOGFILE_PATH, f'stdout_stderr_{submission.id}_{session}')

# Returns Popen subprocess handle and stdout file handle
def start_and_connect_client(session, password, submission, index):
    name = docker_name(session, submission)
    image = submission_docker_tag(submission.id)
    docker_args = [DOCKER_BIN, 'run', '--net=host', '--rm',
            '--name', name,
            '--cpus', CONTAINER_CPU,
            '--memory', CONTAINER_RAM,
            '--memory-swap', CONTAINER_RAM,  # Don't allow swapping to disk
            image]
    run_args = [
            '--server', GAMESERVER_HOST,
            '--port', GAMESERVER_TCPPORT,
            '--password', password,
            '--name', submission.name,
            '--session', session,
            '--index', str(index),
            GAME_NAME]
    stdout_path = match_stdout_path(submission, session)
    logging.info(f'Writing stdout and stderr to {stdout_path}')
    stdout = open(stdout_path, 'w', buffering=1)
    logging.info('Running command: ' + ' '.join(docker_args + run_args))
    p = subprocess.Popen(docker_args + run_args,
            stdout=stdout,
            stderr=subprocess.STDOUT,
            start_new_session=True)  # Do not propagate signals to child processes
    return p, stdout

def wait_for_clients_to_finish(pair, processes, stdouts):
    start_waiting = time.monotonic()
    done = False
    while not done and time.monotonic() < start_waiting + MATCH_TIMEOUT:
        for i, p in enumerate(processes):
            if p.poll() is not None:
                logging.info(f'Client {pair[i].id} done')
                stdouts[i].close()
                done = True
                break
        time.sleep(1)

def kill_remaining_clients(pair, processes, stdouts):
    for i, p in enumerate(processes):
        if p.poll() is None:
            logging.info(f'Killing {pair[i].id}')
            p.terminate()
            time.sleep(5)
            stdouts[i].close()

def upload_file_to_droopy(filename, droopy_filename):
    logging.info(f'Uploading {filename} to {droopy_filename}')
    boundary = 'canyoutellidontlikelibraries'
    headers = {'Content-Type': f'multipart/form-data; boundary={boundary}'}
    if DROOPY_CREDS:
        headers['Authorization'] = 'Basic ' + base64.b64encode(DROOPY_CREDS.encode('utf-8')).decode('utf-8')
    body = f'--{boundary}\nContent-Disposition: form-data; name="upfile"; filename="{droopy_filename}"\n\n'.encode('utf-8')
    body += open(filename, 'rb').read()
    body += f'\n--{boundary}--\n'.encode('utf-8')
    request = urllib.request.Request(DROOPY_URL, data=body, method='POST', headers=headers)
    try:
        urllib.request.urlopen(request)
        return DROOPY_URL + droopy_filename
    except urllib.error.HTTPError as e:
        raise Exception(e.read().decode('utf-8')) from e

def get_match_status(session):
    endpoint = f'http://{GAMESERVER_HOST}:{GAMESERVER_WEBPORT}/status/{GAME_NAME}/{session}'
    logging.info(f'Getting match info from {endpoint}')
    request = urllib.request.Request(endpoint, method='GET')
    try:
        response = urllib.request.urlopen(request)
        return json.loads(response.read())
    except urllib.error.HTTPError as e:
        logging.info(f'POST body: {body}')
        raise Exception(e.read().decode('utf-8')) from e

def wait_for_gameserver_gamelog(session):
    match_status = get_match_status(session)
    tries = 5
    while match_status['status'] != 'over' or match_status['gamelogFilename'] is None:
        tries -= 1
        if tries <= 0:
            raise Exception('Gameserver did not respond with match results')
        time.sleep(tries*1.0)
        match_status = get_match_status(session)
    return match_status

def download_gamelog(gamelog_name):
    log_url = f'http://{GAMESERVER_HOST}:{GAMESERVER_WEBPORT}/gamelog/{gamelog_name}'
    request = urllib.request.Request(log_url, method='GET')
    local_filename = os.path.join(LOGFILE_PATH, gamelog_name)
    try:
        response = urllib.request.urlopen(request)
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(response, f)
        if not os.path.isfile(local_filename):
            raise Exception(f'Could not download gamelog to {local_gamelog_path}')
        return local_filename
    except urllib.error.HTTPError as e:
        logging.info(f'url: {log_url}')
        raise Exception(e.read().decode('utf-8')) from e

def update_game_failed(conn, game_id, reason=''):
    cur = conn.cursor()
    q = '''
UPDATE games
SET
status = 'failed',
win_reason = %s,
lose_reason = %s
WHERE id = %s
    '''
    cur.execute(q, (reason, reason, game_id,))
    conn.commit()

def update_game_succeeded(conn, win_reason, lose_reason, winner_id, log_url, game_id):
    cur = conn.cursor()
    q = '''
UPDATE games
SET
    status = 'finished',
    win_reason = %s,
    lose_reason = %s,
    winner_id = %s,
    log_url = %s
WHERE id = %s
    '''
    cur.execute(q, (win_reason, lose_reason, winner_id, log_url, game_id))
    conn.commit()

def update_game_submission_logs(conn, output_url, game_id, submission_id):
    cur = conn.cursor()
    q = '''
UPDATE games_submissions
SET output_url = %s
WHERE game_id = %s
AND submission_id = %s
    '''
    cur.execute(q, (output_url, game_id, submission_id))
    conn.commit()

if __name__ == '__main__':
    main()
