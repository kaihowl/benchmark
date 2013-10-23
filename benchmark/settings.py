import os

class Settings:

    def __init__(self, name, **kwargs):
        self._name = str(name)
        self._dict = {
            "BLD"                  : "release",
            "WITH_PROFILER"        : None,
            "WITH_V8"              : None,
            "WITH_PAPI"            : None,
            "WITH_MYSQL"           : None,
            "VERBOSE_BUILD"        : None,
            "PERSISTENCY"          : "NONE",
            "NVRAM_MOUNTPOINT"     : None,
            "NVRAM_FILENAME"       : None,
            "NVRAM_FILESIZE"       : None,
            "NVSIMULATOR_FLUSH_NS" : None,
            "NVSIMULATOR_READ_NS"  : None,
            "NVSIMULATOR_WRITE_NS" : None,
            "COMPILER"             : None,
        }
        for k, v in kwargs.iteritems():
            self.__setitem__(k, v)

    def __getitem__(self, key):
        if not self._dict.has_key(key):
            raise Exception("'%s' is not a valid setting" % k)
        else:
            return self._dict[key]

    def __setitem__(self, key, value):
        if not self._dict.has_key(key):
            raise Exception("'%s' is not a valid setting" % k)
        else:
            self._dict[key] = value

    def __str__(self):
        return self.toString()

    def getName(self):
        return self._name

    def toString(self):
        s = "# settings.mk '" + self._name + "'"
        for k, v in self._dict.iteritems():
            if v != None:
                s += "\n%s := %s" % (k, str(v))
        return s

    def writeToFile(self, filename):
        f = open(filename, "w")
        f.write(self.toString())
        f.close()

    def isSameAs(self, filename):
        other = Settings.fromFile(filename)
        for k, v in self._dict.iteritems():
            if str(v) != str(other[k]):
                return False
        return True

    @classmethod
    def fromFile(cls, filename, name=None):
        if not os.path.isfile(filename):
            raise Exception("'%s' is not a valid file" % filename)
        d = {}
        n = name if name != None else os.path.basename(filename)
        for rawline in open(filename):
            line = "".join(rawline.split())
            if line[0] == "#" or len(line) < 5:
                continue
            k, v = line.split(":=")
            d[k] = v
        return cls(n, **d)