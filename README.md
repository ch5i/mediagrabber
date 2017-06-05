# MediaGrabber
A tool intended to run as a scheduled task and to collect photos and videos from different source directories into a target directory, structured by the create date, while keeping records of the target and sources in an index file (database).

## Usage / Command line Options
The following flags and options control the tool's behavior 

Option | Long | Value | Description
:---: | :--- | :--- | :---
-m | --mode | `import` | import files from source dirs to target dir and index
| " |   "    | `index` | validate/update target index
| " |    "    | `reset` | reset sources: remove all source infos (but keep target index)
-s | --sourcedirs | *list of source directories* | list of directory paths to import from (use "" for paths with spaces), separate by spaces
-t | --targetdir | *target directory* | directory to import to and to store the index file.
-e | --extensions | *list of file extensions, separated by spaces* | list of file extensions to import (default: jpg)
-i | --ignore-dirs | *list of patterns, separated by spaces* | exclude patterns to filter subdirectories which should not be imported
-r | --remove-sources | `none` | move (instead of copy) files from source to target
-p | --probe | `none` | do no touch files - preview only)
-q | --quiet | `none` | no processing output to console
-d | --debug | `none` | create detailed debug logfile