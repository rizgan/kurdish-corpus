# Курманджийский корпус

Импортёр собирает документы и предложения из зафиксированной версии Hugging Face JSONL, архива Pewan, OPUS Bianet, двух файлов Interdialect и дампа курдской Wikipedia. Обходчик отдельно пополняет корпус статьями с сайтов из `crawler_sources.json`. Тексты дедуплицируются по нормализованному содержимому и хранятся в SQLite со сжатием zlib; экспорт создаётся как `corpus.jsonl.zst`.

## Установка на Linux

Разместите проект в `/opt/kurdish-corpus`, установите Python и создайте отдельного системного пользователя и каталог данных:

```sh
sudo useradd --system --home-dir /opt/kurdish-corpus --shell /usr/sbin/nologin kurdish-corpus
sudo install -d -o kurdish-corpus -g kurdish-corpus /var/lib/kurdish-corpus
sudo python3 -m venv /opt/kurdish-corpus/venv
sudo env WITH_EXTENSION=0 /opt/kurdish-corpus/venv/bin/pip install -r /opt/kurdish-corpus/requirements.txt
```

`WITH_EXTENSION=0` отключает необязательное C-расширение `mwparserfromhell`; это позволяет установить зависимости на Python 3.14 без GCC.

Импортёр использует `DATA_DIR=/var/lib/kurdish-corpus`. Для импорта Pewan заранее поместите `Pewan.zip` в этот каталог; в архиве ожидается вложенный `Pewan/Corpora/Kurmanji.zip`. Hugging Face, OPUS Bianet, Interdialect и Wikipedia загружаются импортёром автоматически. Состояние и контрольные точки лежат в SQLite и каталоге данных; не удаляйте их для обычного перезапуска.

Установите systemd units и включите таймеры:

```sh
sudo install -o root -g root -m 0644 /opt/kurdish-corpus/deploy/systemd/*.service /opt/kurdish-corpus/deploy/systemd/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now kurdish-corpus-import.timer kurdish-corpus-crawl.timer
sudo systemctl start --no-block kurdish-corpus-import.service kurdish-corpus-crawl.service
```

Импорт запускается ежедневно в 03:20 с небольшим случайным сдвигом. Обходчик запускается снова через час после завершения предыдущего запуска. Оба процесса ограничены 650 MiB RAM и 70% одного CPU. Хранилище приостанавливает запись, когда свободное место падает ниже 1.5 GiB; на сервере с диском 20 GB следите за ростом базы, загруженных источников и экспорта.

## Запуск вручную

```sh
sudo systemctl start kurdish-corpus-import.service
sudo systemctl start kurdish-corpus-crawl.service
sudo journalctl -u kurdish-corpus-import.service -n 100 --no-pager
sudo journalctl -u kurdish-corpus-crawl.service -n 100 --no-pager
```

Пересоздать экспорт после импорта:

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/importer.py export
```

## Сводка корпуса и очереди

Для смешанных веб-источников обходчик использует локальную символьную модель курманджи и зазаки, если её файл существует в каталоге данных. Чтобы воспроизвести обучение на сервере, используйте полный исходный JSONL и команду ниже. При старте обходчик загружает готовую модель один раз. Для обработки статей внешние API моделей не требуются.

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/langid_kurdish.py train \
  --train-per-class 1200 --validation-per-class 300
```

`report.py` запускается вручную, читает базу только для чтения и за один проход считает документы и слова по источникам. Также он показывает состояния очередей и последние ошибки обходчика, объём файлов данных и свободное место раздела. Подсчёт слов читает весь корпус, поэтому запускайте отчёт по необходимости, а не по таймеру.

```sh
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/report.py
sudo -u kurdish-corpus env DATA_DIR=/var/lib/kurdish-corpus \
  /opt/kurdish-corpus/venv/bin/python /opt/kurdish-corpus/report.py --json --sample-errors 10
```
