#!/usr/bin/env python3
"""Check that the bundled JNI libraries run on the oldest systems lz4-java supports.

The CPU baseline and the minimum OS version of a native library are decided by the toolchain that built it (compiler
defaults, and libgcc/CRT objects that end up in the library), not only by the flags in pom.xml. This script inspects
the built libraries instead of trusting the build configuration:

- exactly the expected set of os/arch libraries exists, each in the right binary format;
- no instructions above the CPU baseline of its arch (disassembly or, for RISC-V, the ELF attributes);
- Linux: only libc/ld.so are needed, and no glibc symbol versions newer than the minimum supported glibc;
- macOS: no minimum macOS version above the supported one, only libSystem is needed;
- Windows: only system DLLs are imported.

Usage:
    check_natives.py target/classes/net/jpountz/util
    check_natives.py lz4-java-1.14.0.jar

Disassembling needs a multi-target GNU objdump (binutils-multiarch on Debian/Ubuntu) and, for the macOS libraries,
llvm-objdump. Use --objdump, --llvm-objdump and --readelf to pick other binaries.
"""

import argparse
import collections
import os
import re
import struct
import subprocess
import sys
import tempfile
import zipfile

# x86 instruction sets above the baselines, by the GNU objdump (AT&T) mnemonic
SSE3 = r"addsubp[sd]|hadd[ps][sd]|hsubp[sd]|lddqu|movddup|movs[hl]dup|fisttp[lsq]?"
SSSE3 = r"pshufb|phadd[dsw]w?|phsub[dsw]w?|pmaddubsw|pmulhrsw|psign[bdw]|pabs[bdw]|palignr"
SSE41 = (r"pblendvb|pblendw|blendv?p[sd]|dpp[sd]|extractps|insertps|movntdqa|mpsadbw|packusdw|pcmpeqq|pextr[bdq]"
         r"|pinsr[bdq]|pmaxs[bd]|pmaxu[dw]|pmins[bd]|pminu[dw]|pmov[sz]x[bdw][dqw]|pmuldq|pmulld|ptest|rounds?[ps][sd]"
         r"|phminposuw")
SSE42 = r"pcmp[ei]stri|pcmp[ei]strm|pcmpgtq|crc32[bwlq]?"
POPCNT = r"popcnt[wlq]?"
CX16 = r"cmpxchg16b"
# AVX (any VEX/EVEX encoded vector instruction), AVX-512, BMI1/BMI2/LZCNT/MOVBE
X86_V3_OPERANDS = r"%[yz]mm|%k[0-7]\b"
X86_V3 = r"v[a-z0-9]+(?=[ \t].*%xmm)|(rorx|sarx|shlx|shrx|andn|bextr|blsi|blsmsk|blsr|bzhi|pdep|pext|mulx|lzcnt|movbe)[wlq]?"

# Armv8.1+ (LSE atomics, RCpc, dot product, CRC32 which is optional in 8.0, non-hint pointer authentication, ...), SVE
AARCH64_ABOVE_V8_0 = (r"cas[abhlp]*|ldadd[abhl]*|ldclr[abhl]*|ldeor[abhl]*|ldset[abhl]*|lds(max|min)[abhl]*"
                      r"|ldu(max|min)[abhl]*|swp[abhl]*|ldapr[bh]?|ldapur[bhsw]*|stlur[bh]?|[su]dot|usdot|crc32c?[bhwx]"
                      r"|reta[ab]|bl?ra[ab]z?|ereta[ab]|ldra[ab]|fjcvtzs|frint(32|64)[xz]|bf(dot|mmla|cvt[n2]?)"
                      r"|cpy[fpme][a-z]*|set[pme][a-z]*|sm[34][a-z0-9]*|sha512[a-z0-9]*|eor3|rax1|xar|bcax")
AARCH64_ABOVE_V8_0_OPERANDS = r"\bz[0-9]+\.[bhsdq]\b|\bp[0-9]+/[mz]\b"

# POWER9 (ISA 3.0) and POWER10 (ISA 3.1)
POWER9 = (r"cnttz[dw]\.?|mod[su][dw]|setb|mtvsrdd|mtvsrws|mfvsrld|lxv|stxv|lxvx|stxvx|lxsd|stxsd|lxssp|stxssp"
          r"|lxvb16x|stxvb16x|lxvh8x|stxvh8x|lxvl|lxvll|stxvl|stxvll|lxvwsx|lxsibzx|lxsihzx|stxsibx|stxsihx|xxperm"
          r"|xxpermr|vextu[bhw][lr]x|vcmpnez?[bhw]\.?|vctz[bdhw]|vclzlsbb|vctzlsbb|xxbr[dhqw]|xxspltib|maddld|maddhdu?"
          r"|darn|cmprb|cmpeqb|extswsli\.?|addpcis|vneg[dw]|vprtyb[dqw]|xxinsertw|xxextractuw|vpermr|vrl[dw]nm"
          r"|vrl[dw]mi|vmul10e?c?uq|vbpermd|vextsb2[dw]|vextsh2[dw]|vextsw2d|xs(max|min)[cj]dp|xscmp(eq|ge|gt)dp")
POWER10 = (r"p(ld|lwa|lw[az]|lh[az]|lbz|std|stw|sth|stb|addi|la|lxv|lxsd|lxssp|stxv|stxsd|stxssp|lfs|lfd|stfs|stfd"
           r"|lq|stq|lxvp|stxvp|nop)|lxvpx?|stxvpx?|xxeval|xxspltiw|xxspltidp|xxsplti32dx|xxpermx|xxblendv[bdhw]"
           r"|br[dhw]|setn?bcr?|vstri[bh][lr]\.?|cfuged|pdepd|pextd|cntlzdm|cnttzdm|vextdu[bdhw]vrx|vextdu[bdhw]vlx"
           r"|mtvsr[bdhwq]m|vgnb|vclrlb|vclrrb|vsldbi|vsrdbi|vins[bdhw]|vinsw|vinsd|vcntmb[bdhw]|vexpand[bdhqw]m"
           r"|vextract[bdhqw]m|xvcvbf16spn|xvcvspbf16|xxgenpcv[bdhw]m|xvtlsbb|lxvr[bdhw]x|stxvr[bdhw]x")

# z14 (arch12), z15 (arch13), z16 (arch14): miscellaneous-instruction-extensions 2/3, vector enhancements 1/2,
# guarded storage, NNPA
Z14_AND_LATER = (r"sel[a-z]*|nngrk|nnrk|nogrk|nork|nxgrk|nxrk|ncgrk|ncrk|ocgrk|ocrk|mgrk|msgrkc|msrkc|mgh|agh|sgh"
                 r"|lgg|llgfsg|lgsc|stgsc|vbperm|vllezlf|vmsl|vnn|vnx|voc|vlrlr?|vstrlr?|v?wf(max|min)[a-z]*"
                 r"|vf(max|min)[a-z]*|vfnm[as][a-z]*|wfnm[as][a-z]*|vlbr[a-z]*|vstbr[a-z]*|vler[a-z]*|vster[a-z]*"
                 r"|vlebr[a-z]*|vstebr[a-z]*|vllebrz[a-z]*|vlbrrep[a-z]*|vsld|vsrd|vstrs[a-z]*|vcfps|vcsfp|vcelfb"
                 r"|vclfeb|vcefb|vcfeb|kdsa|dfltcc|sortl|mvcrl|vschp|vschsp|vschdp|vschxp|vcfn|vcnf|vclfn[hl]|vcrnf"
                 r"|nnpa|lbear|stbear|lpswey|kma|prno")

# RISC-V extensions in rv64gc (and the subsets newer assemblers spell out)
RV64GC_EXTENSIONS = {"i", "m", "a", "f", "d", "c", "zicsr", "zifencei", "zmmul", "zaamo", "zalrsc", "zca", "zcd"}


def mnemonics(*alternatives):
    return re.compile(r"^(?:" + "|".join(alternatives) + r")(?=[ \t]|$)")


def operands(pattern):
    return re.compile(pattern)


# Minimum glibc 2.17 (RHEL 7, the first glibc for aarch64 and ppc64le); 2.27 is the first glibc for riscv64
GLIBC_MAX = {"amd64": (2, 17), "i386": (2, 17), "aarch64": (2, 17), "ppc64le": (2, 17), "s390x": (2, 17),
             "riscv64": (2, 27)}
LINUX_NEEDED = re.compile(r"^(libc\.so\.6|ld-linux-[a-z0-9_-]+\.so\.[0-9]+|ld64\.so\.[0-9]+)$")
WINDOWS_IMPORTS = re.compile(r"^(kernel32\.dll|api-ms-win-crt-[a-z0-9-]+\.dll|msvcrt\.dll)$", re.IGNORECASE)
MACOS_DYLIBS = {"/usr/lib/libSystem.B.dylib"}

# os/arch directory -> expected binary format, baseline rules and description of the baseline
ELF_MACHINES = {3: "i386", 62: "x86-64", 183: "aarch64", 21: "ppc64", 22: "s390", 243: "riscv"}
TARGETS = {
    "linux/amd64": dict(fmt=("elf", 64, "little", "x86-64"), baseline="x86-64",
                        rules=[mnemonics(SSE3, SSSE3, SSE41, SSE42, POPCNT, CX16, X86_V3),
                               operands(X86_V3_OPERANDS)]),
    "linux/i386": dict(fmt=("elf", 32, "little", "i386"), baseline="i686 (no MMX/SSE)",
                       rules=[mnemonics(SSE3, SSSE3, SSE41, SSE42, POPCNT, X86_V3),
                              operands(r"%x?mm[0-9]|" + X86_V3_OPERANDS)]),
    "linux/aarch64": dict(fmt=("elf", 64, "little", "aarch64"), baseline="armv8-a",
                          rules=[mnemonics(AARCH64_ABOVE_V8_0), operands(AARCH64_ABOVE_V8_0_OPERANDS)]),
    "linux/ppc64le": dict(fmt=("elf", 64, "little", "ppc64"), baseline="POWER8",
                          rules=[mnemonics(POWER9, POWER10)]),
    "linux/s390x": dict(fmt=("elf", 64, "big", "s390"), baseline="z13", rules=[mnemonics(Z14_AND_LATER)]),
    "linux/riscv64": dict(fmt=("elf", 64, "little", "riscv"), baseline="rv64gc", rules=[]),
    "darwin/x86_64": dict(fmt=("macho", "x86_64"), baseline="core2 (SSSE3)", min_macos=(10, 9),
                          rules=[mnemonics(SSE41, SSE42, POPCNT, X86_V3), operands(X86_V3_OPERANDS)]),
    # Every Apple Silicon Mac is at least an Apple M1
    "darwin/aarch64": dict(fmt=("macho", "arm64"), baseline="apple-m1", min_macos=(11, 0), rules=[]),
    "windows/amd64": dict(fmt=("pe", "x86-64"), baseline="x86-64",
                          rules=[mnemonics(SSE3, SSSE3, SSE41, SSE42, POPCNT, CX16, X86_V3),
                                 operands(X86_V3_OPERANDS)]),
}
EXTENSIONS = {"linux": "so", "darwin": "dylib", "windows": "dll"}


class Checker:
    def __init__(self, objdump, llvm_objdump, readelf):
        self.objdump = objdump
        self.llvm_objdump = llvm_objdump
        self.readelf = readelf
        self.errors = []

    def error(self, target, message):
        self.errors.append((target, message))

    def run(self, *args):
        return subprocess.run(args, check=True, stdout=subprocess.PIPE, universal_newlines=True).stdout

    def check(self, target, path):
        spec = TARGETS[target]
        with open(path, "rb") as f:
            data = f.read()
        kind = spec["fmt"][0]
        actual = {"elf": parse_elf, "macho": parse_macho, "pe": parse_pe}[kind](data)
        if actual is None or actual["fmt"] != spec["fmt"]:
            self.error(target, "expected a %s library, got %s" % (spec["fmt"], actual and actual["fmt"]))
            return
        notes = ["baseline " + spec["baseline"]]

        if kind == "elf":
            notes += self.check_elf(target, path)
        elif kind == "macho":
            notes += self.check_macho(target, spec, actual)
        else:
            notes += self.check_pe(target, path)

        if spec["rules"]:
            # GNU objdump can't read Mach-O on Debian/Ubuntu
            objdump = self.llvm_objdump if kind == "macho" else self.objdump
            self.check_instructions(target, path, objdump, spec["rules"])
        print("%-15s %s" % (target, ", ".join(notes)))

    def check_elf(self, target, path):
        arch = target.split("/")[1]
        notes = []
        needed = re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", self.run(self.readelf, "-d", "-W", path))
        for library in needed:
            if not LINUX_NEEDED.match(library):
                self.error(target, "needs %s, which is not part of glibc" % library)
        notes.append("needs " + " ".join(needed))

        versions = set(re.findall(r"Name: (GLIBC_[A-Z0-9_.]+)", self.run(self.readelf, "-V", "-W", path)))
        newest = (0,)
        for version in versions:
            match = re.match(r"GLIBC_(\d+(?:\.\d+)*)$", version)
            if not match:
                self.error(target, "uses glibc symbol version %s" % version)
                continue
            number = tuple(int(part) for part in match.group(1).split("."))
            newest = max(newest, number)
            if number > GLIBC_MAX[arch]:
                self.error(target, "needs %s, newer than the supported minimum glibc %s"
                           % (version, ".".join(map(str, GLIBC_MAX[arch]))))
        notes.append("glibc >= " + ".".join(map(str, newest)))

        if arch == "riscv64":
            attributes = self.run(self.readelf, "-A", path)
            match = re.search(r'Tag_RISCV_arch: "([^"]*)"', attributes)
            if not match:
                self.error(target, "no Tag_RISCV_arch attribute")
            else:
                # rv64i2p1_m2p0_..._zicsr2p0: base ISA, then extensions with their versions
                extensions = [re.sub(r"\d+p\d+$", "", part) for part in match.group(1).split("_")]
                extensions[0] = extensions[0][len("rv64"):]
                above = [ext for ext in extensions if ext not in RV64GC_EXTENSIONS]
                if above:
                    self.error(target, "built for extensions above rv64gc: %s" % " ".join(above))
        return notes

    def check_macho(self, target, spec, info):
        notes = []
        if info["min_macos"] is None:
            self.error(target, "no minimum macOS version")
        elif info["min_macos"] > spec["min_macos"]:
            self.error(target, "minimum macOS %s is newer than %s" % (version_string(info["min_macos"]),
                                                                       version_string(spec["min_macos"])))
        else:
            notes.append("macOS >= " + version_string(info["min_macos"]))
        for dylib in info["dylibs"]:
            if dylib not in MACOS_DYLIBS:
                self.error(target, "needs %s, which is not part of macOS" % dylib)
        return notes

    def check_pe(self, target, path):
        imports = re.findall(r"DLL Name: (\S+)", self.run(self.objdump, "-p", path))
        for dll in imports:
            if not WINDOWS_IMPORTS.match(dll):
                self.error(target, "imports %s, which is not part of Windows" % dll)
        return ["imports " + " ".join(imports)]

    def check_instructions(self, target, path, objdump, rules):
        disassembly = self.run(objdump, "-d", "--no-show-raw-insn", path)
        hits = collections.Counter()
        functions = collections.defaultdict(set)
        function = None
        instructions = 0
        for line in disassembly.splitlines():
            match = re.match(r"^[0-9a-f]+ <(.*)>:$", line)
            if match:
                function = match.group(1)
                continue
            match = re.match(r"^\s*[0-9a-f]+:\s+(\S.*)$", line)
            if not match:
                continue
            instructions += 1
            # Some objdump targets separate mnemonic and operands with a tab, others with spaces
            instruction = match.group(1).replace("\t", " ").strip()
            if any(rule.search(instruction) for rule in rules):
                mnemonic = instruction.split(" ")[0]
                hits[mnemonic] += 1
                functions[mnemonic].add(function)
        if instructions == 0:
            self.error(target, "objdump produced no instructions; does it support this architecture?")
        for mnemonic, count in sorted(hits.items()):
            self.error(target, "%dx %s above the baseline, in %s"
                       % (count, mnemonic, ", ".join(sorted(f or "?" for f in functions[mnemonic])[:5])))


def parse_elf(data):
    if data[:4] != b"\x7fELF":
        return None
    bits = {1: 32, 2: 64}.get(data[4])
    order = {1: "little", 2: "big"}.get(data[5])
    machine = struct.unpack_from("<H" if order == "little" else ">H", data, 18)[0]
    return {"fmt": ("elf", bits, order, ELF_MACHINES.get(machine, "machine %d" % machine))}


def parse_pe(data):
    if data[:2] != b"MZ":
        return None
    offset = struct.unpack_from("<I", data, 0x3c)[0]
    if data[offset:offset + 4] != b"PE\0\0":
        return None
    machine = struct.unpack_from("<H", data, offset + 4)[0]
    return {"fmt": ("pe", {0x8664: "x86-64", 0x14c: "i386", 0xaa64: "arm64"}.get(machine, "machine %#x" % machine))}


def parse_macho(data):
    if data[:4] != b"\xcf\xfa\xed\xfe":  # 64-bit little endian
        return None
    cputype, _, _, ncmds = struct.unpack_from("<iIII", data, 4)
    cpu = {0x01000007: "x86_64", 0x0100000c: "arm64"}.get(cputype, "cputype %#x" % cputype)
    min_macos = None
    dylibs = []
    offset = 32
    for _ in range(ncmds):
        cmd, size = struct.unpack_from("<II", data, offset)
        if cmd == 0x32:  # LC_BUILD_VERSION
            platform, minos = struct.unpack_from("<II", data, offset + 8)
            if platform == 1:  # PLATFORM_MACOS
                min_macos = decode_macos_version(minos)
        elif cmd == 0x24:  # LC_VERSION_MIN_MACOSX
            min_macos = decode_macos_version(struct.unpack_from("<I", data, offset + 8)[0])
        elif cmd in (0xc, 0x80000018, 0x8000001f, 0x80000023):  # LC_LOAD_DYLIB, _WEAK_, LC_REEXPORT_, LC_LOAD_UPWARD_
            name_offset = struct.unpack_from("<I", data, offset + 8)[0]
            name = data[offset + name_offset:offset + size].split(b"\0", 1)[0]
            dylibs.append(name.decode())
        offset += size
    return {"fmt": ("macho", cpu), "min_macos": min_macos, "dylibs": dylibs}


def decode_macos_version(value):
    return (value >> 16, (value >> 8) & 0xff)


def version_string(version):
    return ".".join(map(str, version))


def find_libraries(root):
    found = {}
    for os_id in sorted(os.listdir(root)):
        if not os.path.isdir(os.path.join(root, os_id)):
            continue
        for arch in sorted(os.listdir(os.path.join(root, os_id))):
            directory = os.path.join(root, os_id, arch)
            if not os.path.isdir(directory):
                continue
            for name in os.listdir(directory):
                if name.startswith("liblz4-java."):
                    found.setdefault(os_id + "/" + arch, []).append(os.path.join(directory, name))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", help="net/jpountz/util directory with <os>/<arch>/liblz4-java.* or a lz4-java jar")
    parser.add_argument("--objdump", default="objdump")
    parser.add_argument("--llvm-objdump", default="llvm-objdump")
    parser.add_argument("--readelf", default="readelf")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as temp:
        root = args.path
        if os.path.isfile(root):
            with zipfile.ZipFile(root) as jar:
                for name in jar.namelist():
                    if re.match(r"^net/jpountz/util/[^/]+/[^/]+/liblz4-java\.[a-z]+$", name):
                        jar.extract(name, temp)
            root = os.path.join(temp, "net", "jpountz", "util")

        checker = Checker(args.objdump, args.llvm_objdump, args.readelf)
        found = find_libraries(root) if os.path.isdir(root) else {}
        for target in sorted(set(found) | set(TARGETS)):
            os_id = target.split("/")[0]
            expected = "liblz4-java." + EXTENSIONS.get(os_id, "?")
            paths = found.get(target, [])
            if target not in TARGETS:
                checker.error(target, "unexpected native library: %s" % ", ".join(paths))
            elif [os.path.basename(p) for p in paths] != [expected]:
                checker.error(target, "expected exactly %s, found %s"
                              % (expected, ", ".join(os.path.basename(p) for p in paths) or "nothing"))
            else:
                try:
                    checker.check(target, paths[0])
                except (OSError, subprocess.CalledProcessError) as e:
                    checker.error(target, "could not inspect %s: %s" % (paths[0], e))

    for target, message in checker.errors:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print("::error::%s: %s" % (target, message))
        else:
            print("ERROR %s: %s" % (target, message), file=sys.stderr)
    return 1 if checker.errors else 0


if __name__ == "__main__":
    sys.exit(main())
