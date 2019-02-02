from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import argparse
import urllib2
from threading import Thread
import multiprocessing as mp
import numpy as np
from glob import glob
import pdb


"""Prepare
"""
FLAGS = None

classes = ['drawings', 'hentai', 'neutral', 'porn', 'sexy']

"""Member Function
"""
def load_from_url(img_url, out_file_path):
    resp = urllib2.urlopen(img_url, timeout=30)
    respHtml = resp.read()
    binfile = open(out_file_path, "wb")
    binfile.write(respHtml)
    binfile.close()


def save_img(img_url, out_file_path):
    try:
        # downloading
        print("Loading : %s" % img_url)
        load_from_url(img_url, out_file_path)
        return True

    except Exception as e:
        count = 1
        while count <= 5:
            try:
                load_from_url(img_url, out_file_path)
                break
            except Exception as e:
                err_info = 'Reloading for %d time' % count if count == 1 else 'Reloading for %d times' % count
                print(err_info)
                count += 1
        if count > 5:
            print("Downloading %s failed." % img_url)

        return False


def get_sub_url_list(url_list, idx, sz):
    num_of_url = len(url_list)
    left = idx * sz
    right = np.min([(idx + 1) * sz, num_of_url - 1])
    return url_list[left:right]


def thread_download_img(pid, tid, urls_list, out_img_dir, num_of_image_in_process):

    for idx, url in enumerate(urls_list):
        img_name = url.strip().split('/')[-1]
        img_full_path = os.path.join(out_img_dir, img_name)

        if os.path.exists(img_full_path):
            print("Image %s exist! Skip ..." % img_name)
            continue
        else:
            success_download = save_img(url, img_full_path)
            if success_download:
                print("[%d|%d]-[%d/%d] success download: %s" % (pid, tid, idx, num_of_image_in_process, url))


def process_download_img(pid, urls_list, out_img_dir, num_of_thread):
    num_of_img_in_process = len(urls_list)

    sub_list_size = int(num_of_img_in_process / num_of_thread)

    threads = []
    for tid in range(num_of_thread):
        sub_list = get_sub_url_list(urls_list, tid, sub_list_size)

        t = Thread(target=thread_download_img, args=[pid, tid, sub_list, out_img_dir, num_of_img_in_process])

        t.start()
        threads.append(t)

    for t in threads:
        t.join()


"""Main
"""
def main():
    source_url_dir = FLAGS.source_url_dir
    output_root_dir = FLAGS.output_image_dir

    num_processes = FLAGS.num_of_processes
    num_threads_in_process = FLAGS.num_of_threads

    for cls in classes:

        # get url path
        url_file_dir = os.path.join(source_url_dir, cls)
        url_file_path = os.path.join(url_file_dir, 'urls_' + cls + '.txt')

        # get image saving dir
        out_img_dir = os.path.join(output_root_dir, cls, 'IMAGES')
        if not os.path.exists(out_img_dir):
            print("folder [ %s ] is not exist, make a new one" % out_img_dir)
            os.makedirs(out_img_dir)

        # get the list of image names in the out_img_dir (images which had been loaded)
        exist_img_path_list = glob(os.path.join(out_img_dir, '*.jpg'))
        exist_img_names_list = [x.split('/')[-1] for x in exist_img_path_list]

        if os.path.exists(url_file_path):
            # get url list
            f = open(url_file_path, 'r')
            urls_list = f.readlines()
            f.close()
            num_of_image = len(urls_list)

            # unload image
            unload_image_url_list = []
            for url in urls_list:
                image_name = url.strip().split('/')[-1]
                if image_name in exist_img_names_list:
                    pass
                else:
                    unload_image_url_list.append(url)

            num_of_unload_image = len(unload_image_url_list)

            print("Total num of %s image is %d!" % (cls, num_of_image))
            print("Unload num of %s image is %d" % (cls, num_of_unload_image))

            #pdb.set_trace()

            # if too few, no need to use multi-process-threads
            if num_of_unload_image < 200:
                num_processes = 1
                if num_of_unload_image < 50:
                    num_threads_in_process = 1

            """Multi process
            """
            sub_list_size_in_process = int(num_of_unload_image / num_processes)

            jobs = []
            for pid in range(num_processes):
                sub_list = get_sub_url_list(unload_image_url_list, pid, sub_list_size_in_process)

                worker = mp.Process(target=process_download_img,
                                    args=(pid, sub_list, out_img_dir, num_threads_in_process))
                #process_download_img(pid, sub_list, out_img_dir, num_threads_in_process)

                worker.start()
                jobs.append(worker)

            for job in jobs:
                job.join()

        else:
            print('%s is missing!' % url_file_path)
            return


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # url root dir
    parser.add_argument('--source_url_dir', type=str,
                        default='/Users/fulun/Project/nsfw_data_scrapper/raw_data',
                        required=False,
                        help='input dir of image url.')
    # output root dir
    parser.add_argument('--output_image_dir', type=str,
                        default='/Users/fulun/Project/nsfw_data_scrapper/raw_data',
                        required=False,
                        help='output dir of image.')

    # num of process
    parser.add_argument('--num_of_processes', type=int,
                        default=2,
                        required=False,
                        help='num of process')

    # num of threads in process
    parser.add_argument('--num_of_threads', type=int,
                        default=6,
                        required=False,
                        help='num of threads in process')

    FLAGS, _ = parser.parse_known_args()

    print("FLAGS:%s\n" % FLAGS)

    # pdb.set_trace()

    main()