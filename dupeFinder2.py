#!/usr/bin/env python3
# required pip modules:
# click==6.7
# guessit==2.1.1
import hashlib
import multiprocessing
import os
import pathlib
import timeit
from functools import partial
from multiprocessing import Pool, Manager

import click
from guessit import guessit

pool = None
file_list = []


@click.command()
@click.option('--path', prompt=None, help='Path to scan for duplicate movies / tv episodes.')
@click.option('--list', default=None,
              help='Path of a plaintext file with filepaths seperated by a new line. '
                   'This is scanned for duplicate movies / tv episodes.')
@click.option('--workers', default=multiprocessing.cpu_count(),
              help='Processes to use for processing files. (Default: ' + str(multiprocessing.cpu_count()) + ')')
@click.option('--save_dupes', default=None, help='Path of file to save duplicates too.')
@click.option('--save_skipped', default=None, help='Path of file to save skipped files too.')
@click.option('--save_unprocessed', default=None, help='Path of file to save unprocessed files too.')
@click.option('--tree', is_flag=True,
              help="Use tree to create a filepaths list and load that. Use with --path")
def dupefinder(path, list, workers, save_dupes, save_skipped, save_unprocessed, tree):
    """ Find duplicate Movies / TV Episodes from a specified folder """

    if path is None and list is None:
        click.echo("You specify a --path or --list to scan, use --help for more information.")
        exit(0)

    # Build a list of file paths to scan
    if path is not None:
        # Build list of files to process
        start_time = timeit.default_timer()
        if tree and tree_installed():
            # Use tree to create a filepaths list and load that instead
            click.echo("Building a list of file paths with tree...")
            os.system("tree -fi -n -o filepath_treelist.log \"" + path + "\"")
            load_file_list("filepath_treelist.log")
        else:
            # Use os.walk to build filepaths list
            click.echo("Building a list of file paths...")
            build_file_list(path)
        time_taken = timeit.default_timer() - start_time
        click.echo("Built list of files in %d seconds" % time_taken)
    elif list is not None:
        # Build list of files from path_list
        click.echo("Loading file paths from list...")
        start_time = timeit.default_timer()
        load_file_list(list)
        time_taken = timeit.default_timer() - start_time
        click.echo("Loaded list of files in %d seconds" % time_taken)

    # Scan for duplicates
    click.echo("Checking %d files for duplicate movies / tv episodes..." % len(file_list))
    start_time = timeit.default_timer()
    pool = Pool(processes=workers)
    manager = Manager()
    file_hash_map = manager.dict(lock=True)
    dupes_hash_map = manager.dict(lock=True)
    unprocessed_map = manager.list()
    skipped_map = manager.list()
    ns = manager.Namespace()
    ns.movies = 0
    ns.episodes = 0
    ns.dupe_count = 0
    ns.unprocessed = 0
    ns.non_videos = 0
    ns.videos = 0
    pool.map(
        partial(process_file, hash_map=file_hash_map, dupes=dupes_hash_map, unprocessed=unprocessed_map,
                skipped=skipped_map, namespace=ns), file_list)
    time_taken = timeit.default_timer() - start_time

    # Show scan results
    click.echo("Finished checking for duplicates, it took %d seconds" % time_taken)
    click.echo("Videos checked: %d" % ns.videos)
    click.echo("Skipped non videos: %d" % ns.non_videos)
    click.echo("Unprocessed files: %d" % ns.unprocessed)
    if ns.movies:
        click.echo("Movies: %d" % ns.movies)
    if ns.episodes:
        click.echo("Episodes: %d" % ns.episodes)
    click.echo("Duplicates: %d" % ns.dupe_count)

    # Save detected dupes to file
    if save_dupes is not None and len(dupes_hash_map):
        lines_wrote = 0
        with open(save_dupes, 'w') as f:
            for v in dupes_hash_map.values():
                if isinstance(v, str):
                    f.write(v + '\n')
                    lines_wrote += 1
            click.echo("Saved %d dupes to %s" % (lines_wrote, save_dupes))
            f.close()

    # Save skipped files to file
    if save_skipped is not None and len(skipped_map):
        lines_wrote = 0
        with open(save_skipped, 'w') as f:
            for v in skipped_map:
                if isinstance(v, str):
                    f.write(v + '\n')
                    lines_wrote += 1
            click.echo("Saved %d skipped files to %s" % (lines_wrote, save_skipped))
            f.close()

    # Save unprocessed files to file
    if save_unprocessed is not None and len(unprocessed_map):
        lines_wrote = 0
        with open(save_unprocessed, 'w') as f:
            for v in unprocessed_map:
                if isinstance(v, str):
                    f.write(v + '\n')
                    lines_wrote += 1
            click.echo("Saved %d unprocessed files to %s" % (lines_wrote, save_unprocessed))
            f.close()


def tree_installed():
    installed = False

    try:
        ret = os.popen('tree --version').read()
        if ret.startswith("tree v"):
            return True
    except:
        return installed

    return installed


def load_file_list(path_list):
    with open(path_list, encoding="utf8") as f:
        for line_terminated in f:
            if '.' in line_terminated.rstrip('\n')[1:]:
                file_list.append(line_terminated.rstrip('\n'))


def build_file_list(folder):
    for path, subdirs, files in os.walk(folder):
        for name in files:
            file = pathlib.PurePath(path, name)
            file_list.append(file)


whitelist_containers = ['m4v', 'ts']


def process_file(path, hash_map, dupes, unprocessed, skipped, namespace):
    try:
        file_info = guessit(path.__str__())

        if file_info is not None:
            if ('mimetype' in file_info and 'video/' in file_info['mimetype']) or 'video_codec' in file_info or (
                            'container' in file_info and file_info['container'] in whitelist_containers):
                namespace.videos += 1
                if file_info['type'] == 'episode':
                    if 'alternative_title' in file_info:
                        key = "{0}{1}".format(file_info['title'].lower(), file_info['alternative_title'].lower())
                    else:
                        key = "{0}".format(file_info['title'].lower())

                    if 'season' in file_info and 'episode' in file_info:
                        key += "{0}x{1}".format(file_info['season'], file_info['episode'])
                    elif 'date' in file_info:
                        key += str(file_info['date'])
                    elif 'part' in file_info:
                        key += str(file_info['part'])
                    else:
                        click.echo("Not sure how to process this episode: %s" % path)
                        namespace.unprocessed += 1
                        unprocessed.append(path)
                        return None

                    if 'country' in file_info:
                        key += str(file_info['country'])

                    if 'year' in file_info:
                        key += str(file_info['year'])

                    if 'edition' in file_info:
                        key += str(file_info['edition'])

                elif file_info['type'] == 'movie':
                    if 'other' in file_info and file_info['other'] == 'Trailer':
                        click.echo("Skipping trailer: %s" % path)
                        namespace.unprocessed += 1
                        unprocessed.append(path)
                        return None

                    key = file_info['title']
                    if 'year' in file_info:
                        key += str(file_info['year'])
                    if 'cd' in file_info:
                        key += str(file_info['cd'])

                else:
                    click.echo("Not sure how to process: %s" % path)
                    namespace.unprocessed += 1
                    unprocessed.append(path)
                    return None

                key_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
                if key_hash not in hash_map:
                    # not a duplicate video
                    hash_map[key_hash] = path
                    if file_info['type'] == 'episode':
                        namespace.episodes += 1
                    elif file_info['type'] == 'movie':
                        namespace.movies += 1
                else:
                    # duplicate video
                    dupes[key_hash + path] = path
                    namespace.dupe_count += 1
                    click.echo("Duplicate found: %s" % path)
                    if key_hash not in dupes:
                        dupes[key_hash] = hash_map[key_hash]
                        click.echo("Duplicate found: %s" % hash_map[key_hash])
                        namespace.dupe_count += 1

            else:
                namespace.non_videos += 1
                skipped.append(path)
                return None

    except Exception as ex:
        namespace.unprocessed += 1
        unprocessed.append(path)
        return None


if __name__ == "__main__":
    dupefinder()
