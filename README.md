# urfiles

## Introduction

urfiles creates a searchable database of files and file attributes.

## Tape archive file format

Tape archive files are stored in a directory of the same name as the label on
the tape. This name is stored in the ARCHIVE environment variable in the
examples below.

Two files are read from this directory, md5sum.txt and stat.txt.

### md5sum.txt

This file contains the output of md5sum for each file on the tape. It is
produced with the following commands:

    find $ARCHIVE -type f -print0 | xargs -0 -L10 -P72 md5sum > md5sum.tmp
    sort < md5sum.tmp > $ARCHIVE/md5sum.txt
    rm md5sum.tmp

### stat.txt

This file contains the output of the stat command for each file on the
tape. It is produced with the following commands:

    find $ARCHIVE -type f -print0 | xargs -0 -L10 -P72 \
      stat --format='%n %1.1F %a %s %u %g %Y %y' > stat.tmp
    sort < stat.tmp > $ARCHIVE/stat.txt
    rm stat.tmp

Of note, this isn't a particularly good format for automatic parsing since the
file name comes first and may contain spaces (i.e., it would have been better
to start each line with fields that cannot contain spaces, and perhaps have
the filename at the end).

## activate and deactivate

I use the following zsh macros:

    activate () {
        export VIRTUAL_ENV="$(pwd -P)"
        _OLD_VIRTUAL_PATH="$PATH"
        PATH="$VIRTUAL_ENV/bin:$PATH"
        hash -r
        _OLD_VIRTUAL_PROMPT="$PROMPT"
        PROMPT="%{%B%F{cyan}%}py:$(basename $VIRTUAL_ENV)%{%f%b%} $PROMPT"
    }

    deactivate () {
        if [ -n "$_OLD_VIRTUAL_PATH" ]
        then
                PATH="$_OLD_VIRTUAL_PATH"
                unset _OLD_VIRTUAL_PATH
        fi
        hash -r
        if [ -n "$_OLD_VIRTUAL_PROMPT" ]
        then
                PS1="$_OLD_VIRTUAL_PROMPT"
                unset _OLD_VIRTUAL_PROMPT
        fi
        unset VIRTUAL_ENV
        unset PYTHONHOME
        unset PYTHONPATH
    }
