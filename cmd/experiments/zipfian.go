package main

import (
	"time"
	"math/rand"
	"math"
)


type Zipfian struct {
	items int64
	alpha, zetaN, eta, theta float64
	r *rand.Rand
}

func NewZipfian(items int64, constant float64) Zipfian {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return NewZipfianWithRand(items, constant, rng)
}

func NewZipfianWithRand(items int64, constant float64, r *rand.Rand) Zipfian {
	zetaN := zeta(items, constant)
	zeta2theta := zeta(2, constant)

	return Zipfian {
		items: items,
		theta: constant,
		alpha: 1.0/(1.0 - constant),
		zetaN: zetaN,
		eta: (1 - math.Pow(2.0/float64(items), 1-constant))/(1 - zeta2theta / zetaN),
		r: r,
	}
}

func (z *Zipfian) NextItem() int64 {
	u := z.r.Float64()
	uz := u * z.zetaN

	if uz < 1.0 {
		return 0
	}

	if uz < 1.0 + math.Pow(0.5, z.theta) {
		return 1
	}

	return int64(float64(z.items)*math.Pow(z.eta*u-z.eta+1, z.alpha))
}

func (z * Zipfian) reset(items int64) {
	zeta2theta := zeta(2, z.theta)
	z.items = items
	z.zetaN = zeta(items, z.theta)
	z.eta = (1 - math.Pow(2.0/float64(z.items), 1-z.theta))/(1 - zeta2theta / z.zetaN)
}

func zeta(n int64, theta float64) float64 {
	sum := 0.0;
	for i := int64(0); i < n; i++ {
		sum += 1 / math.Pow(float64(i+1), theta);
	}
	return sum
}

type ZipfianSamples struct {
	sources []int64
	counts []int64
	first int64
	samples int64
	zipf Zipfian
}

func NewZipfianSamples(nsources int64, samples int64, zipf Zipfian) ZipfianSamples {
	sources := make([]int64, nsources)
	for i := 0; i < len(sources); i++ {
		sources[i] = int64(i)
	}
	return ZipfianSamples {
		sources: sources,
		counts: make([]int64, len(sources)),
		first: 0,
		samples: samples,
		zipf: zipf,
	}
}

func (z * ZipfianSamples) NextItem() *int64 {
	if len(z.sources) == 0 {
		return nil
	} else {
		idx := z.zipf.NextItem()
		source := z.sources[idx]
		z.counts[source] += 1
		if z.counts[source] == z.samples {
			l := len(z.sources)
			z.sources[idx] = z.sources[l-1] // move the last item to the removed position
			z.sources = z.sources[:l-1] // then reslice
			//z.sources = append(z.sources[:idx], z.sources[idx+1:]...)
			if len(z.sources) > 0 {
				z.zipf.reset(int64(len(z.sources)))
			}
		}
		return &source
	}
	return nil
}



