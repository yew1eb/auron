import sys

def dedup_lines(in_path: str, out_path: str):
    seen = set()
    with open(in_path, "r", encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout:
        for line in fin:
            if line not in seen:
                seen.add(line)
                fout.write(line)

if __name__ == "__main__":
    print("111")
    if len(sys.argv) != 2:
        println("Usage: python dedup_lines.py <input_file>")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = input_file + ".dedup.txt"
    dedup_lines(input_file, output_file)
    print(f"Deduplicated lines written to: {output_file}")