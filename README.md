# Kurmanji Corpus

[Русская версия](README.ru.md)

This project collects Kurmanji articles, documents and sentences. The importer reads a pinned Hugging Face JSONL release, the Pewan archive, OPUS Bianet, two Interdialect files and the Kurdish Wikipedia dump. The crawler adds articles from the sites listed in `crawler_sources.json`. The store deduplicates records by normalized text, compresses them with zlib in SQLite and exports them to `corpus.jsonl.zst`.

## Linux setup

Place the repository in `/opt/kurdish-corpus` and install Python 3 with venv support. Create the service account, data directory and virtual environment.

```sh
sudo useradd --system --home-dir /opt/kurdish-corpus --shell /usr/sbin/nologin kurdish-corpus
sudo install -d -o kurdish-corpus -g kurdish-corpus /var/lib/kurdish-corpus
sudo python3 -m venv /opt/kurdish-corpus/venv
sudo env WITH_EXTENSION=0 /opt/kurdish-corpus/venv/bin/pip install -r /opt/kurdish-corpus/requirements.txt
```

`WITH_EXTENSION=0` disables the optional C extension for `mwparserfromhell`, allowing installation on Python 3.14 without GCC.

The services use `DATA_DIR=/var/lib/kurdish-corpus`. To include Pewan, place `Pewan.zip` there with the nested archive `Pewan/Corpora/Kurmanji.zip` intact. The importer fetches the Hugging Face, OPUS Bianet, Interdialect and Wikipedia sources automatically. Keep the data directory and SQLite checkpoints when restarting so the importer and crawler can resume.

Install the systemd units, enable the timers and start the first run.

```sh
sudo install -o root -g root -m 0644 /opt/kurdish-corpus/deploy/systemd/*.service /opt/kurdish-corpus/deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kurdish-corpus-import.timer kurdish-corpus-crawl.timer
sudo systemctl start --no-block kurdish-corpus-import.service kurdish-corpus-crawl.service
```

The import runs daily at 03:20 server time with a randomized delay. The crawler starts again one hour after its previous run ends. Each service is limited to 650 MiB of RAM and 70% of one CPU core. The store stops writing when free disk space falls below 1.5 GiB. On a 20 GB disk, monitor the combined size of the database, downloaded sources and export.

## Manual runs and logs

```sh
sudo systemctl start kurdish-corpus-import.service
sudo systemctl start kurdish-corpus-crawl.service
sudo journalctl -u kurdish-corpus-import.service -n 100 --no-pager
sudo journalctl -u kurdish-corpus-crawl.service -n 100 --no-pager
```

To rebuild the export after an import, run

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/importer.py export
```

## Language filtering

For mixed-language sources, the crawler can use a local character n-gram model to distinguish Kurmanji from Zazaki. Train it with the complete source JSONL in the data directory. The crawler loads the saved model at startup and processes articles locally.

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/langid_kurdish.py train \
  --train-per-class 1200 --validation-per-class 300
```

## Corpus and queue reports

Run `report.py` for a read-only summary of document and word counts by source, queue states, recent crawl errors, data directory size and free disk space. Word counts require a full scan of the corpus. The second command prints JSON and includes up to ten error samples.

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/report.py
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/report.py --json --sample-errors 10
```
