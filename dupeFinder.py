#!/usr/bin/env python3
import hashlib
import logging
import os
import pathlib
import sys
import timeit
from logging.handlers import RotatingFileHandler

import curio
from guessit import guessit

############################################################
# INIT
############################################################

# Setup logging
logFormatter = logging.Formatter('%(asctime)s - %(message)s')
rootLogger = logging.getLogger()

consoleHandler = logging.StreamHandler()
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

fileHandler = RotatingFileHandler('output.log', maxBytes=1024 * 1024 * 5, backupCount=5)
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

logger = rootLogger.getChild("DUPE_FINDER")
logger.setLevel(logging.DEBUG)

# Variables
files = {}
dupes = {}
exceptions = []
non_videos = []


############################################################
# METHODS
############################################################

async def process_file(file):
    try:
        file_info = guessit(file.__str__())

        if file_info is not None:
            if 'mimetype' in file_info and 'video/' in file_info['mimetype']:
                # logger.debug("Processing file: %s", file)

                if file_info['type'] == 'episode':
                    if 'alternative_title' in file_info:
                        key = "{0}{1}{2}x{3}".format(file_info['title'].lower(), file_info['alternative_title'].lower(),
                                                     file_info['season'], file_info['episode'])
                    else:
                        key = "{0}{1}x{2}".format(file_info['title'].lower(), file_info['season'], file_info['episode'])
                    if 'country' in file_info:
                        key += str(file_info['country'])

                elif file_info['type'] == 'movie':
                    key = file_info['title']
                    if 'year' in file_info:
                        key += str(file_info['year'])
                    if 'cd' in file_info:
                        key += str(file_info['cd'])

                else:
                    logger.debug("Not sure how to process: %s", file)
                    exceptions.append(file)
                    return None

                key_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
                if key_hash not in files:
                    # not a duplicate video
                    files[key_hash] = file
                else:
                    # duplicate video
                    logger.debug("Duplicate video found: %s", file)
                    dupes[file] = files[key_hash]

            else:
                non_videos.append(file)

    except Exception as ex:
        exceptions.append(file)
        # logger.exception("Exception processing file: %s", file)
        return None


async def find_dupes(folder):
    logger.debug("Scanning folder: %s", folder)

    for path, subdirs, files in os.walk(folder):
        for name in files:
            file = pathlib.PurePath(path, name)
            await process_file(file.__str__())

    return None


############################################################
# MAIN ENTRY
############################################################

if __name__ == "__main__":
    folder = ''

    if len(sys.argv) < 2:
        logger.debug("You must specify a path, e.g. dupeFinder.py /home/seed/media/TV")
        exit()
    else:
        folder = sys.argv[1]

    start_time = timeit.default_timer()
    results = curio.run(find_dupes(folder), with_monitor=True)
    time_taken = timeit.default_timer() - start_time

    logger.debug("Finished looking for dupes!\n\n")
    logger.debug("Time taken: %d seconds", time_taken)
    logger.debug("Skipped non videos: %d", len(non_videos))
    logger.debug("Videos scanned: %d", len(files) + len(dupes))
    logger.debug("Non duplicates: %d", len(files))
    logger.debug("Duplicates: %d\n\n", len(dupes))

    if len(dupes):
        logger.debug("Dupes:")
        for file, dupe_of in dupes.items():
            logger.debug("%s is a duplicate of %s", file, dupe_of)
        logger.debug("\n\n")

    if len(exceptions):
        logger.debug("Couldn't process:")
        for file in exceptions:
            logger.debug(file)
