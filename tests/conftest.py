import pytest
from curio import Kernel
from colonel import Colonel 

@pytest.fixture(scope='session')
def kernel(request):
    k = Colonel()
    request.addfinalizer(lambda: k.run(shutdown=True))
    return k
