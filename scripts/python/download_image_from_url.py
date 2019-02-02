from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import argparse
import urllib2
from threading import Thread
import numpy as np
import pdb


"""Prepare
"""
FLAGS = None

classes = ['drawings', 'hentai', 'neutral', 'porn', 'sexy']

use_multi_threads = True

NUM_OF_THREAD = 10


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


def thread_download_img(urls_list, out_img_dir, num_of_image):

    for idx, url in enumerate(urls_list):
        img_name = url.strip().split('/')[-1]
        img_full_path = os.path.join(out_img_dir, img_name)

        if os.path.exists(img_full_path):
            print("Image %s exist! Skip ..." % img_name)
            continue
        else:
            success_download = save_img(url, img_full_path)
            if success_download:
                print("[%d/%d] success download: %s" % (idx, num_of_image, url))


"""Main Function
"""
def main():
    source_url_dir = FLAGS.source_url_dir
    output_root_dir = FLAGS.output_image_dir

    for cls in classes:

        # get url path
        url_file_dir = os.path.join(source_url_dir, cls)
        url_file_path = os.path.join(url_file_dir, 'urls_' + cls + '.txt')

        # get image saving dir
        out_img_dir = os.path.join(output_root_dir, cls, 'IMAGES')
        if not os.path.exists(out_img_dir):
            print("folder [ %s ] is not exist, make a new one" % out_img_dir)
            os.makedirs(out_img_dir)

        if os.path.exists(url_file_path):
            # get url list
            f = open(url_file_path, 'r')
            urls_list = f.readlines()
            f.close()
            num_of_image = len(urls_list)

            if use_multi_threads:
                """Multi threads
                """
                num_of_thread = 10
                sub_list_size = int(num_of_image/num_of_thread)

                threads = []
                for tid in range(num_of_image):
                    sub_list = get_sub_url_list(urls_list, tid, sub_list_size)
                    num_of_sub_list = len(sub_list)

                    t = Thread(target=thread_download_img, args=[sub_list, out_img_dir, num_of_sub_list])
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join()

            else:
                """Single thread
                """
                # failed loaded image logging
                faild_log_file = os.path.join(url_file_dir, 'failed_%s.txt' % cls)
                failed_log_f = open(faild_log_file, 'w')

                for idx, url in enumerate(urls_list):
                    img_name = url.strip().split('/')[-1]
                    img_full_path = os.path.join(out_img_dir, img_name)

                    if os.path.exists(img_full_path):
                        print("Image %s exist! Skip ..." % img_name)
                        continue
                    else:
                        success_download = save_img(url, img_full_path)
                        if success_download:
                            print("[%d/%d] success download: %s" % (idx, num_of_image, url))
                        else:
                            failed_log_f.writelines(url)

                failed_log_f.close()

        else:
            print('%s is missing!' % url_file_path)
            return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # OSS Buckets
    parser.add_argument('--source_url_dir', type=str,
                        default='/Users/fulun/Project/nsfw_data_scrapper/raw_data',
                        required=False,
                        help='input dir of image url.')
    # Model Checkpoint
    parser.add_argument('--output_image_dir', type=str,
                        default='/Users/fulun/Project/nsfw_data_scrapper/raw_data',
                        required=False,
                        help='output dir of image.')

    FLAGS, _ = parser.parse_known_args()

    print("FLAGS:%s\n" % FLAGS)

    # pdb.set_trace()

    main()

