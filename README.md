# Youtube crawling

아래 파일들의 경로를 적절히 수정할 것.

`MMTrail2M_half1.json`: Youtube video id가 포함된 json file.

`download_video` function 내부의 `video_dir`: 다운로드된 데이터가 저장될 경로.

`FAILED_LOG`: 다운로드 실패한 video id를 저장한 txt file. (동영상 삭제, 시청제한 등)

`COMPLETED_LOG`: 다운로드 완료된 video id를 저장한 txt file.

`common_opts` 내부의 `cookie_file`: 크롤링을 위한 쿠키 정보가 저장된 txt file.

`cookies.txt`는 별도 과정을 통해 확보해야 함.
