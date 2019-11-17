PYRENA:


Pyrena is a small arena for siggame/cadre written in Python


REQUIREMENTS:


Before using this arena, you must do the following:
1. Setup a postgres server and create the tables defined in https://github.com/siggame/ophelia/blob/develop/db/init.sql
2. Setup a siggame gameserver https://github.com/siggame/Cerveau
    - Ideally this would run on the same machine as the `pyrena` so that all network calls are local, though this is not a requirement
3. Install the psycopg2 python3 module `pip install psycopg2`
4. Install docker on the machine running `Pyrena`


COMPONENTS:

                                                    ____ Gameserver (Cerveau)
                                                   /
tournament_scheduler _                 _________pyrena________
                      \ PostgreSQL DB /                       \ droopygz (large file storage)
webserver backend ____/               \____client web app_____/
               \______________________________/

Tournament_scheduler : Schedules games for N-elimination tournaments in the BD
Webserver backend    : Serves API used by client web app
PostgreSQL DB        : Stores state for users, teams, games, etc.
Pyrena               : Reads scheduled games from database, sets up docker VMs and runs them against each other using Gamerserver, uploads logs to droopyz
Gameserver          : Listens for game clients, validates commands, updates clients, generates gamelogs
Client web app       : Authenticated user-friendly interface for uploading submissions, viewing gamelogs, etc.
Droopygz             : Stores and serves large gzip-compressed files


PROVISIONING:


General procedure to setup a fresh pyrena VM:
1. Create VM with
    - ubuntu 1804
    - recommended 25GB boot disk (for gameserver's gamelogs)
2. Allow access to database from this new VM if using IP-based DB whitelisting
3. SSH into machine and install packages:

sudo apt-get update
sudo apt-get install tmux
sudo apt-get install python3-psycopg2
git clone https://github.com/tarnasa/pyrena
git clone https://github.com/siggame/joueur
sudo apt-get install docker.io
sudo groupadd docker
sudo usermod -a -G docker $USER

4. Setup the gameserver

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
*new shell*
nvm install 9.11
npm install
npm run-script build
vim node_modules/@types/express-serve-static-core/index.d.ts
line 900, replace fn: BLABLABLA with fn: Function
echo '#!/bin/sh' > run_gameserver
echo npm run start -- --arena --visualizer-url=http://vis.codegames.ai --only-load blobmaster >> run_gameserver
chmod +x run_gameserver

5. Start the gameserver

./run_gameserver

5. Create a run file for pyrena, something similar to:

#!/bin/sh
export LOOKBACK_SECONDS=60
export GAME_NAME=Chess
export MATCH_TIMEOUT=280
export DOCKERFILE_PATH=/home/coolcat/joueur
export DB_NAME=postgres
export DB_USER=ophelia
export DB_PASS='blablabla'
export DB_HOST=123.123.123.123
export DROOPY_URL=http://drop.megaminerai.games/
export LOGFILE_PATH=/home/coolcat/pyrena_logs
export SUBMISSION_CACHE_PATH=/home/coolcat/submission_cache
export DROOPY_CREDS=
export RUN_FOREVER=True
export CONTAINER_CPU=1
export CONTAINER_RAM=1g
python3 pyrena.py

6. Test the pyrena by running it with RUN_FOREVER=False
Pyrena will:

1. Read from database a list of teams and submissions
2. Write a new row into GAMES and GAMES_SUBMISSIONS table
3. Download zip files for clients
4. Unzip
5. Verify the directory structure
6. Replace Dockerfile in the submission using language-specific file in DOCKERFILE_PATH
7. Build the docker container for each client
8. Upload build log to droopy and DB with path to file
9. Setup a session in the gameserver
10. Start the docker containers and connect them to the gameserver
11. Poll gameserver till match is over or timeout
12. Upload gamelog, client logs to droopy
13. Update GAMES table with fail or success and link to gamelog
14. Quit

The pyrena provides lots of logging, so any errors should be easily apparent.

7. Run pyrena continuously

Using the run-file above
Pyrena will repeat the above steps until until a fatal error, such as the disk filling up.

8. Run a tournament

Mark all teams you want in the tournament as eligible.
The tournament schedule will use the latest submitted version for each team.

GAME_NAME=Chess \
DB_HOST=\
DB_PORT=\
DB_NAME=\
DB_USER=\
DB_PASS=\
N_ELIMINATION=3\
BEST_OF=7\
python3 tournament_scheduler.py
