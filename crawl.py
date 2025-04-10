import yt_dlp
import os
import multiprocessing
import json
from tqdm import tqdm
from multiprocessing import Pool

FAILED_LOG = "failed_ids.txt"
COMPLETED_LOG = "complete_folders.txt"

# 🔧 실패한 video_id 불러오기
def load_failed_ids():
    if os.path.exists(FAILED_LOG):
        with open(FAILED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

# 🔧 성공한 video_id 불러오기
def load_completed_ids():
    if os.path.exists(COMPLETED_LOG):
        with open(COMPLETED_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f)
    return set()

# 🔧 실패 기록
def log_failed(video_id, error_msg=""):
    with open(FAILED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{video_id}\n")
    print(f"[ERROR] {video_id} 실패 기록됨. 사유: {error_msg}")

# 🔧 성공 기록
def log_completed(video_id):
    with open(COMPLETED_LOG, "a", encoding="utf-8") as f:
        f.write(f"{video_id}\n")

def download_video(video_id):
    if video_id in failed_ids or video_id in completed_ids:
        print(f">>> {video_id} 이전에 실패 or 완료됨. 건너뜁니다.")
        return

    url = f'https://www.youtube.com/watch?v={video_id}'
    # video_dir = os.path.join('downloads', video_id)
    video_dir = os.path.join('/mnt/hdd8tb/downloads', video_id)
    os.makedirs(video_dir, exist_ok=True)

    mp4_path = os.path.join(video_dir, f'{video_id}.mp4')
    mp3_path = os.path.join(video_dir, f'{video_id}_audio.mp3')
    json_path = os.path.join(video_dir, f'{video_id}.info.json')

    mp4_exists = os.path.exists(mp4_path)
    mp3_exists = os.path.exists(mp3_path)
    json_exists = os.path.exists(json_path)

    if mp4_exists and mp3_exists and json_exists:
        print(f'>>> {video_id} 모든 파일 이미 있음. 건너뜁니다.')
        log_completed(video_id)
        return

    try:
        def run_dl(opts, desc):
            print(f'>>> {video_id} - {desc} 다운로드 중...')
            with yt_dlp.YoutubeDL(opts) as ydl:
                ydl.download([url])
            print(f'>>> {video_id} - {desc} 완료!\n')

        common_opts = {
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'ignoreerrors': True,
            'cookiefile': '/home/daeyong/llm_music_understanding/youtube_crawl/cookies.txt',
        }

        if not mp4_exists:
            video_opts = {
                **common_opts,
                'outtmpl': os.path.join(video_dir, f'{video_id}.%(ext)s'),
                'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4',
                'merge_output_format': 'mp4',
            }
            run_dl(video_opts, "MP4")

        if not mp3_exists:
            audio_opts = {
                **common_opts,
                'outtmpl': os.path.join(video_dir, f'{video_id}_audio.%(ext)s'),
                'format': 'bestaudio/best',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
            }
            run_dl(audio_opts, "MP3")

        if not json_exists:
            metadata_opts = {
                **common_opts,
                'skip_download': True,
                'writeinfojson': True,
                'outtmpl': os.path.join(video_dir, f'{video_id}.%(ext)s'),
            }
            run_dl(metadata_opts, "Metadata")

        # ✅ 다운로드 후 파일 3개 모두 있는지 검증
        if os.path.exists(mp4_path) and os.path.exists(mp3_path) and os.path.exists(json_path):
            log_completed(video_id)
        else:
            log_failed(video_id, "다운로드 후 파일 일부 누락됨")

    except Exception as e:
        log_failed(video_id, str(e))

if __name__ == '__main__':
    json_path = "MMTrail2M_half1.json"
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    video_ids = [item['video_id'] for item in data]

    # 🔥 실패 or 완료된 ID 필터링
    failed_ids = load_failed_ids()
    completed_ids = load_completed_ids()
    video_ids = [vid for vid in video_ids if vid not in failed_ids and vid not in completed_ids]

    # 병렬 처리 + 진행 바
    num_workers = 2
    with Pool(num_workers) as pool:
        with tqdm(total=len(video_ids), desc="다운로드 진행") as pbar:
            for _ in pool.imap_unordered(download_video, video_ids):
                pbar.update(1)
