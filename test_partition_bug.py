from pathlib import Path
from internal.partition import Partition

log_dir = Path("test_partition_offset_bug")
log_dir.mkdir(exist_ok=True)

# 1. Create partition and append some messages
p = Partition("test_topic", 0, log_dir)
p.append({"data": "msg1"})  # offset 0
p.append({"data": "msg2"})  # offset 1
p.append({"data": "msg3"}, offset=10)  # offset 10 (skipping 2-9)

print(f"After first run, next_offset is: {p.next_offset}")  # Should be 11

# 2. Re-open the partition (simulating restart)
p2 = Partition("test_topic", 0, log_dir)
print(
    f"After restart, next_offset is: {p2.next_offset}"
)  # If it just counted lines, it will be 3!

p2.append({"data": "msg4"})
print(f"Appended msg4, new next_offset is: {p2.next_offset}")

# Read all back
for msg in p2.read_all():
    print(msg)
