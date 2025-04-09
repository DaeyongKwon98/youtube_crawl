import yt_dlp
import os
import json
import multiprocessing
from tqdm import tqdm
from multiprocessing import Queue, Process

BASE_DIR = "/mnt/hdd8tb"
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")

FAILED_LOG = "failed_ids.txt"
COMPLETED_LOG = "complete_folders.txt"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def load_ids(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

def log(path, video_id):
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{video_id}\n")

def download_mp4(video_id):
    url = f"https://www.youtube.com/watch?v={video_id}"
    video_dir = os.path.join(DOWNLOAD_DIR, video_id)
    os.makedirs(video_dir, exist_ok=True)

    mp4_path = os.path.join(video_dir, f"{video_id}.mp4")
    if os.path.exists(mp4_path):
        return mp4_path

    opts = {
        'quiet': True,
        'no_warnings': True,
        'noplaylist': True,
        'ignoreerrors': True,
        'cookiefile': '/home/daeyong/llm_music_understanding/youtube_crawl/cookies.txt',
        'outtmpl': mp4_path,
        'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
        'merge_output_format': 'mp4',
        'concurrent_fragment_downloads': 3,
        'downloader_args': {'http_chunk_size': '1M'},
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        return mp4_path
    except Exception as e:
        print(f"[MP4 ERROR] {video_id}: {e}")
        return None

def extract_mp3(video_id):
    video_dir = os.path.join(DOWNLOAD_DIR, video_id)
    mp4_path = os.path.join(video_dir, f"{video_id}.mp4")
    mp3_path = os.path.join(video_dir, f"{video_id}_audio.mp3")
    if os.path.exists(mp3_path):
        return mp3_path

    opts = {
        'quiet': True,
        'no_warnings': True,
        'cookiefile': '/home/daeyong/llm_music_understanding/youtube_crawl/cookies.txt',
        'outtmpl': mp3_path,
        'format': 'bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        return mp3_path
    except Exception as e:
        print(f"[MP3 ERROR] {video_id}: {e}")
        return None

def download_metadata(video_id):
    json_path = os.path.join(DOWNLOAD_DIR, video_id, f"{video_id}.info.json")
    if os.path.exists(json_path):
        return json_path

    opts = {
        'quiet': True,
        'no_warnings': True,
        'cookiefile': '/home/daeyong/llm_music_understanding/youtube_crawl/cookies.txt',
        'outtmpl': os.path.join(DOWNLOAD_DIR, video_id, f"{video_id}.%(ext)s"),
        'skip_download': True,
        'writeinfojson': True,
    }

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])
        return json_path
    except Exception as e:
        print(f"[JSON ERROR] {video_id}: {e}")
        return None

def mp4_download_worker(video_id_queue, mp4_done_queue):
    while True:
        video_id = video_id_queue.get()
        if video_id is None:
            break

        mp4_path = download_mp4(video_id)
        if mp4_path:
            mp4_done_queue.put(video_id)
        else:
            log(FAILED_LOG, video_id)

def postprocess_worker(mp4_done_queue):
    while True:
        video_id = mp4_done_queue.get()
        if video_id is None:
            break

        mp3_path = extract_mp3(video_id)
        json_path = download_metadata(video_id)

        video_dir = os.path.join(DOWNLOAD_DIR, video_id)
        mp4_path = os.path.join(video_dir, f"{video_id}.mp4")

        if all(os.path.exists(p) for p in [mp4_path, mp3_path, json_path]):
            log(COMPLETED_LOG, video_id)
        else:
            log(FAILED_LOG, video_id)

if __name__ == '__main__':
    # json_path = "MMTrail2M.json"
    json_path = "MMTrail2M_half1.json"
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    video_ids = [item['video_id'] for item in data]

    failed_ids = load_ids(FAILED_LOG)
    completed_ids = load_ids(COMPLETED_LOG)

    filtered_ids = [vid for vid in video_ids if vid not in failed_ids and vid not in completed_ids]

    num_download_workers = 4
    num_post_workers = 2

    video_id_queue = multiprocessing.Queue()
    mp4_done_queue = multiprocessing.Queue()

    downloaders = [Process(target=mp4_download_worker, args=(video_id_queue, mp4_done_queue)) for _ in range(num_download_workers)]
    postprocessors = [Process(target=postprocess_worker, args=(mp4_done_queue,)) for _ in range(num_post_workers)]

    for p in downloaders + postprocessors:
        p.start()

    for vid in tqdm(filtered_ids, desc="Queueing video_ids"):
        video_id_queue.put(vid)

    for _ in downloaders:
        video_id_queue.put(None)
    for p in downloaders:
        p.join()

    for _ in postprocessors:
        mp4_done_queue.put(None)
    for p in postprocessors:
        p.join()
