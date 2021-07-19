#!/usr/bin/env python3
# identify.py -*-python-*-

import hashlib
import json
import os
import re
import subprocess

try:
    # There are several different versions of magic available in the open
    # source community. This version is 0.4.20, which is packaged by Debian.
    # magic.version() reports 539. Other versions currently available include
    # python-magic 5.19.1 and filemagic 1.6. I did not explore these.
    import magic
except ImportError as e:
    print('''\
# Cannot import magic: {}
# Consider: apt-get install python3-magic'''.format(e))
    raise SystemExit from e

try:
    import pymediainfo
except ImportError as e:
    print('''\
# Cannot import pymediainfo: {}
# Consider: apt-get install python3-pymediainfo'''.format(e))
    raise SystemExit from e

# pylint: disable=unused-import
from urfiles.log import DEBUG, INFO, ERROR, FATAL


class Identify():
    def __init__(self, file, block_size=2**20, debug=False):
        self.file = file
        self.block_size = block_size
        self.debug = debug

    @staticmethod
    def _add(result, field, data, append=False, force=False):
        if data is not None:
            if isinstance(data, str) and len(data) > 20:
                data = re.sub(' /.*', '', data)
            if force or field not in result:
                result[field] = data
            elif append:
                result[field] = result[field] + ',' + data

    def _extract_general(self, result, track):
        self._add(result, 'encoded_date', track.encoded_date)
        self._add(result, 'tagged_date', track.tagged_date)
        self._add(result, 'format', track.format)
        self._add(result, 'rating', track.rating)
        self._add(result, 'duration', track.duration)
        self._add(result, 'performer', track.performer)
        self._add(result, 'album', track.album)
        self._add(result, 'track', track.track_name)

    def _extract_video(self, result, track):
        self._add(result, 'video_codec', track.codec_id)
        self._add(result, 'video_codec', track.commercial_name)
        self._add(result, 'width', track.width)
        self._add(result, 'height', track.height)

    def _extract_audio(self, result, track):
        self._add(result, 'audio_codec', track.codec_id)
        self._add(result, 'audio_codec', track.commercial_name)
        self._add(result, 'audio_mode', track.bit_rate_mode)
        self._add(result, 'audio_rate', track.bit_rate)

    def _extract_image(self, result, track):
        self._add(result, 'width', track.width)
        self._add(result, 'height', track.height)

    def _extract_text(self, result, track):
        self._add(result, 'language', track.language, append=True)

    def mediainfo(self):
        result = dict()
        mi = pymediainfo.MediaInfo.parse(self.file, cover_data=True)
        if self.debug:
            print(json.dumps(json.loads(mi.to_json()), indent=4,
                             sort_keys=False))
        for track in mi.tracks:
            if track.track_type == 'General':
                self._extract_general(result, track)
            if track.track_type == 'Video':
                self._extract_video(result, track)
            if track.track_type == 'Audio':
                self._extract_audio(result, track)
            if track.track_type == 'Image':
                self._extract_image(result, track)
            if track.track_type == 'Text':
                self._extract_text(result, track)
        return result

    def exinfo(self):
        result = dict()
        proc = subprocess.run(['exiftool', '-c', '%f', '-j',
                               self.file],
                              capture_output=True,
                              text=True,
                              check=False)
        if proc.returncode != 0:
            return result

        metadata = json.loads(proc.stdout)[0]
        if self.debug:
            print(json.dumps(metadata, indent=4, sort_keys=False))

        # For PDFs
        self._add(result, 'pdf_version', metadata.get('PDFVersion', None))
        self._add(result, 'pages', metadata.get('PageCount', None))
        self._add(result, 'title', metadata.get('Title', None))
        self._add(result, 'author', metadata.get('Author', None))
        self._add(result, 'create_date', metadata.get('CreateDate', None))
        self._add(result, 'isbn', metadata.get('ISBN', None))

        # For images
        self._add(result, 'width', metadata.get('ImageWidth', None),
                  force=True)
        self._add(result, 'height', metadata.get('ImageHeight', None),
                  force=True)
        self._add(result, 'camera', metadata.get('Model', None))
        self._add(result, 'lens', metadata.get('LensModel', None))
        self._add(result, 'shutter', metadata.get('ShutterSpeed', None))
        self._add(result, 'aperture', metadata.get('Aperture', None))
        self._add(result, 'focal_length', metadata.get('FocalLength', None))
        self._add(result, 'gps_date', metadata.get('GPSDateTime', None))
        self._add(result, 'gps_lon', metadata.get('GPSLongitude', None))
        self._add(result, 'gps_lat', metadata.get('GPSLatitude', None))
        return result

    def md5(self):
        checksum = hashlib.md5()
        with open(self.file, 'rb') as f:
            while True:
                data = f.read(self.block_size)
                if not data:
                    break
                checksum.update(data)
        return checksum.hexdigest()

    def id(self, checksum=True):
        result = dict()
        md5 = 0
        if os.path.isfile(self.file):
            result['type'] = 'file'
            result.update(self.mediainfo())
            if 'format' not in result:
                result['magic'] = magic.from_file(self.file)

            # Was originall only for PDF, JPEG, TIFF, and PNG, but might as
            # well get exif for every file
            result.update(self.exinfo())

            if checksum:
                md5 = self.md5()
        elif os.path.isdir(self.file):
            result['type'] = 'directory'
        elif os.path.islink(self.file):
            result['type'] = 'link'
        else:
            result['type'] = 'unknown'
        return md5, result
