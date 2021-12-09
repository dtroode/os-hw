# Мультипроцессорный парсинг новостной ленты вконтакте

Программа работает в два процесса.

## Первый процесс (клиент)

Получает несколько постов через API, в три потока записывает их в JSON файлы:

1. Первый поток записывает текст новости,
2. Второй поток записывает прикреплённые изображения новости,
3. Третий поток записывает ссылки и хэштеги новости.

Есть также четвёртый поток, который иногда считывает данные из этих файлов. Он пытается получить блокировку на файл, блокировка поулчается поочерёдно на первый, второй и третий файлы. Если блокировка не получена, поток ждёт неограниченное количество времени. Он прекращает свою работу, когда три основных потока завершили работу – это реализовано через барьеры.

## Второй процесс (сервер)

В три потока считывает данные из JSON файлов и параллельно записывает в базу данных SQLite. Для записи используются три таблицы, соответствующие трём файлам.

Его можно запустить как обычную программу, а также можно запустить как демона, тогда он будет работать постоянно.

Для запуска второго процесса как демона в macOS используются Launch Services. Файл `settings/com.dtroode.vk-daemon.plist` хранит настройки для запуска. Он помещается в `~/Library/LaunchAgents`. После этого выполняются две команды:

```
launchctl bootstrap gui/<uid> com.dtroode.vk-daemon.plist
launchctl kickstart -k gui/<uid>/com.dtroode.vk-daemon
```

Здесь `<uid>` – айди пользователя в системе. Его можно узнать при помощи команды `id -u`. Первая команда используется для добавление службы, вторая для немедленного запуска.

Для удаления службы из списка используется команда:

```
launchctl bootout gui/<uid> com.dtroode.vk-daemon.plist
```

Интервал, указанный в настройках равен суткам в секундах – программа будет запускаться раз в сутки. Также в настройках указаны файлы для логов.

## Связь процессов

Процессы связаны через сокеты. Второй процесс постоянно прослушивает на заданном адресе и порте. Когда он получает сигнал от первого процесса, начинает свою работу, первый процесс в это время ждёт ответный сигнал.

**Оба процесса могут работать независимо.** Первый процесс работает даже без подключения к серверу, на каждой итерации межпроцессорного взаимодействия он пытается подключиться повторно. Второй процесс постоянно слушает, прерывание первого процесса не влияет на работу второго.