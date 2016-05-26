
import os

import enum

class PATH_TYPE (enum.Enum):
    UNKNOWN = -1
    MISSING = 0
    FILE = 1
    DIRECTORY = 2

def path_type (path):
    if (not os.path.exists(path)):
        return PATH_TYPE.MISSING
    elif (os.path.isfile(path)):
        return PATH_TYPE.FILE
    elif (os.path.isdir(path)):
        return PATH_TYPE.DIRECTORY
    else:
        return PATH_TYPE.UNKNOWN

def path_mtime (path):
    path_type_ = path_type(path)

    if (path_type_ == PATH_TYPE.MISSING) or (path_type_ == PATH_TYPE.UNKNOWN):
        return None

    elif (path_type_ == PATH_TYPE.FILE):
        return os.path.getmtime(path)

    elif (path_type_ == PATH_TYPE.DIRECTORY):
        latest_mtime, visited_paths = 0, {}
        for (current_path, subfolders, filenames) in os.walk(path, followlinks = True):
            current_path = os.path.realpath(current_path)
            if (current_path in visited_paths):
                del subfolders[:]
                continue

            for filename in filenames:
                # we ignore system files
                if (filename.startswith('.')):
                    continue

                filename = os.path.join(current_path, filename)

                # we ignore broken symbolic links
                if (not os.path.isfile(filename)):
                    continue

                mtime = os.path.getmtime(filename)
                if (mtime > latest_mtime):
                    latest_mtime = mtime

            visited_paths[current_path] = True

        return latest_mtime
