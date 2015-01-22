from nose.tools import eq_, raises

import fmn.lib.tests
import fmn.lib.multiproc


class TestMultiproc(fmn.lib.tests.Base):
    def test_single_map(self):
        pool = fmn.lib.multiproc.FixedPool(1)
        try:
            def fn(x):
                return x + 2
            pool.target(fn)
            results = pool.apply(range(2))
            eq_(set(results), set([2, 3]))
        finally:
            pool.close()

    def test_multi_map(self):
        pool = fmn.lib.multiproc.FixedPool(5)
        try:
            def fn(x):
                return x + 2
            pool.target(fn)
            results = pool.apply(range(10))
            eq_(set(results), set([2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
        finally:
            pool.close()

    def test_is_targeted(self):
        pool = fmn.lib.multiproc.FixedPool(5)
        eq_(pool.targeted, False)
        try:
            def fn(x):
                return x + 2
            pool.target(fn)
            eq_(pool.targeted, True)
        finally:
            pool.close()

    @raises(ValueError)
    def test_target_before_apply(self):
        pool = fmn.lib.multiproc.FixedPool(5)
        pool.apply([1, 2, 3])


    def test_reuse(self):
        pool = fmn.lib.multiproc.FixedPool(5)

        def fn1(x):
            return x + 1

        def fn2(x):
            return x + 2

        try:
            pool.target(fn1)
            results = pool.apply([1, 2])
            eq_(set(results), set([2, 3]))
        finally:
            pool.close()

        try:
            pool.target(fn2)
            results = pool.apply([1, 2])
            eq_(set(results), set([3, 4]))
        finally:
            pool.close()

    def test_inner_exception(self):
        pool = fmn.lib.multiproc.FixedPool(5)

        error_message = "oh no!"

        def fn(x):
            raise ValueError(error_message)

        try:
            pool.target(fn)
            pool.apply(['whatever'])
            assert False
        except ValueError as e:
            assert error_message in e.message
        finally:
            pool.close()
