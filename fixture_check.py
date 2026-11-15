from pathlib import Path
assert Path('CONTRACT.md').stat().st_size > 0
print('Public contract only; target business capability is not implemented.')
