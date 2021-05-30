# urfiles

## Introduction

urfiles creates a searchable database of files and file attributes.

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
