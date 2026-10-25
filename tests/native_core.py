"""Check the C ABI against independent arbitrary-precision integer arithmetic."""
import ctypes
import random
import sys

class Checked(ctypes.Structure):
    _fields_=[('valid',ctypes.c_int32),('value',ctypes.c_int64)]

class CheckedUnsigned(ctypes.Structure):
    _fields_=[('valid',ctypes.c_int32),('value',ctypes.c_uint64)]

def main(path):
    library=ctypes.CDLL(path)
    library.cairn_verified_abi_version.restype=ctypes.c_uint32
    assert library.cairn_verified_abi_version()==2
    successor=library.cairn_next_incarnation
    successor.argtypes=[ctypes.c_uint64]
    successor.restype=CheckedUnsigned
    unsigned_edges=[0,1,2**32-1,2**32,2**53+1,2**63,2**64-2,2**64-1]
    for current in unsigned_edges:
        result=successor(current)
        assert result.valid==int(current<2**64-1)
        if result.valid: assert result.value==current+1
    guard=library.cairn_may_use_open_state
    guard.argtypes=[ctypes.c_int32,ctypes.c_uint64,ctypes.c_uint64,ctypes.c_uint64]
    guard.restype=ctypes.c_int32
    for present in (0,1,2):
        for current in unsigned_edges:
            for stored in unsigned_edges:
                for requested in unsigned_edges:
                    assert guard(present,current,stored,requested)==int(present==1 and current==stored==requested)
    add=library.cairn_checked_add_i64
    add.argtypes=[ctypes.c_int64,ctypes.c_int64]
    add.restype=Checked
    edges=[-2**63,-2**63+1,-2**53-1,-32768,-1,0,1,32767,2**53+1,2**63-2,2**63-1]
    generator=random.Random(19473)
    pairs=[(a,b) for a in edges for b in edges]
    pairs += [(generator.randrange(-2**63,2**63),generator.randrange(-2**63,2**63)) for _ in range(10000)]
    for a,b in pairs:
        result=add(a,b)
        expected=a+b
        valid=-2**63<=expected<2**63
        assert result.valid==int(valid),(a,b,result.valid)
        if valid: assert result.value==expected,(a,b,result.value,expected)
    print(f'verified C ABI: {len(pairs)} arithmetic comparisons, unsigned successors and 1536 incarnation guards passed',flush=True)

if __name__=='__main__': main(sys.argv[1])
