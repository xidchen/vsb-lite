import argparse
import multiprocessing
import os
import re
import shutil
import subprocess
import sys
import time

import tqdm

import cfg


def process_files(
    video_dir, files, subtitle_dir, video_sub_dir, vn_mapping, vs_mapping
):
    for video_file in files:
        original_dir = os.path.dirname(os.path.realpath(__file__))
        video_no_sub_from_path = os.path.join(video_dir, video_file)
        video_no_sub_to_path = os.path.join(video_sub_dir, video_file)
        os.rename(video_no_sub_from_path, video_no_sub_to_path)
        video_name = vn_mapping[video_file]
        subtitle_file = vs_mapping[video_file]
        subtitle_from_path = os.path.join(subtitle_dir, subtitle_file)
        subtitle_to_path = os.path.join(video_sub_dir, subtitle_file)
        os.rename(subtitle_from_path, subtitle_to_path)
        video_sub_file = video_name + cfg.video_translated_sub_suffix
        os.chdir(video_sub_dir)
        duration_cmd = [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", video_file
        ]
        duration_process = subprocess.Popen(
            duration_cmd, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True
        )
        duration_output, _ = duration_process.communicate()
        duration = float(duration_output.strip())
        with tqdm.tqdm(
            desc=f"Processing {video_file}", total=int(duration)
        ) as pbar:
            cmd = [
                "ffmpeg", "-y", "-i", video_file, "-vf",
                f"subtitles={subtitle_file}", video_sub_file
            ]
            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True
            )
            for line in process.stderr:
                progress = parse_progress(line, duration)
                if progress is not None:
                    pbar.update(progress)
            process.wait()
            pbar.update(int(duration) - pbar.n % int(duration))
        os.chdir(original_dir)
        os.rename(video_no_sub_to_path, video_no_sub_from_path)
        os.rename(subtitle_to_path, subtitle_from_path)


def parse_progress(line, duration):
    """
    Parse ffmpeg output to extract progress percentage.
    """
    if "frame=" in line and "fps=" in line:
        match = re.search(r"time=(\d+):(\d+):(\d+.\d+)", line)
        if match:
            hours, minutes, seconds = map(float, match.groups())
            total_seconds = hours * 3600 + minutes * 60 + seconds
            progress = int((total_seconds / duration) * 100)
            return progress
    return None


def nice_time_cost(time_cost):
    hours, minutes = divmod(time_cost, 3600)
    minutes, seconds = divmod(minutes, 60)
    if hours:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes:
        return f"{int(minutes)}m {int(seconds)}s"
    elif seconds:
        return f"{int(seconds)}s"


if __name__ == "__main__":
    root_dir = os.path.dirname(os.path.realpath(__file__))
    multiprocessing.set_start_method("spawn")
    parser = argparse.ArgumentParser(description="video subtitle burner")
    parser.add_argument(
        "--video-no-sub-dir",
        default=cfg.video_no_sub_dir,
        help="video no subtitle directory path"
    )
    parser.add_argument(
        "--translated-sub-dir",
        default=cfg.translated_sub_dir,
        help="translated subtitle directory path"
    )
    parser.add_argument(
        "--video-translated-sub-dir",
        default=cfg.video_translated_sub_dir,
        help="video translated subtitle directory path"
    )
    args = vars(parser.parse_args())
    video_no_sub_dir = args["video_no_sub_dir"]
    translated_sub_dir = args["translated_sub_dir"]
    video_translated_sub_dir = args["video_translated_sub_dir"]
    video_no_sub_dir = str(os.path.join(root_dir, video_no_sub_dir))
    translated_sub_dir = str(os.path.join(root_dir, translated_sub_dir))
    video_translated_sub_dir = str(os.path.join(root_dir, video_translated_sub_dir))
    if not os.path.exists(video_no_sub_dir):
        print(f"Video directory does not exist: {video_no_sub_dir}")
        sys.exit(0)
    if not os.path.exists(translated_sub_dir):
        print(f"Subtitle directory does not exist: {translated_sub_dir}")
        sys.exit(0)
    video_no_sub_files = os.listdir(video_no_sub_dir)
    translated_sub_files = os.listdir(translated_sub_dir)
    if not video_no_sub_files:
        print("Video directory is empty")
        sys.exit(0)
    if not translated_sub_files:
        print("Subtitle directory is empty")
        sys.exit(0)
    video_name_mapping = {
        f: f.replace(cfg.video_no_sub_suffix, "")
        for f in os.listdir(video_no_sub_dir)
    }
    sub_name_mapping = {
        f.replace(cfg.translated_sub_suffix, ""): f
        for f in os.listdir(translated_sub_dir)
    }
    video_sub_mapping = {}
    for video_no_sub_file in video_no_sub_files:
        if video_name_mapping[video_no_sub_file] in sub_name_mapping:
            sub_name = video_name_mapping[video_no_sub_file]
            video_sub_mapping[video_no_sub_file] = sub_name_mapping[sub_name]
        else:
            print(f"Video does not have a subtitle file: {video_no_sub_file}")
            sys.exit(0)
    if os.path.exists(video_translated_sub_dir):
        shutil.rmtree(video_translated_sub_dir)
    os.makedirs(video_translated_sub_dir)
    num_videos = len(video_no_sub_files)
    num_processes = min(num_videos, multiprocessing.cpu_count())
    chunk_size = num_videos // num_processes
    remainder = num_videos % num_processes
    chunks = []
    chunk_start = 0
    for i in range(num_processes):
        if i < remainder:
            chunk_end = chunk_start + chunk_size + 1
        else:
            chunk_end = chunk_start + chunk_size
        chunks.append(video_no_sub_files[chunk_start:chunk_end])
        chunk_start = chunk_end
    all_start_time = time.time()
    pool = multiprocessing.Pool(processes=num_processes)
    pool.starmap(
        process_files,
        [
            (video_no_sub_dir, chunk, translated_sub_dir,
             video_translated_sub_dir, video_name_mapping, video_sub_mapping)
            for chunk in chunks
        ]
    )
    pool.close()
    pool.join()
    all_end_time = time.time()
    all_time_cost = all_end_time - all_start_time
    print(
        f"Subtitles of all {num_videos} videos burned "
        f"within {nice_time_cost(all_time_cost)}"
    )
