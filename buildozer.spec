[app]

# Название вашего приложения
title = CallParser

# Имя пакета (обязательно уникально)
package.name = callparser

# Домен пакета (также должно быть уникальным)
package.domain = org.example

# Главный Python-файл
source.main = main.py

# Логотип (иконка)
icon.filename = %(source.dir)s/data/icon.png

# (Включаем pyjnius, pyrogram и т.д.)
requirements = python3, kivy, pyjnius, pycryptodome, tgcrypto, pyrogram, requests

# Разрешения, необходимые приложению
# READ_PHONE_STATE нужно для чтения состояния телефона и номера.
android.permissions = READ_PHONE_STATE, READ_CALL_LOG, INTERNET

# Если нужно дополнительно, можно добавить запись в AndroidManifest для отслеживания PHONE_STATE
# Но в примере мы будем делать динамическую регистрацию ресивера из Python
# Если хотите статическую – смотрите раздел service и broadcastreceiver в документации Buildozer.

# ...
