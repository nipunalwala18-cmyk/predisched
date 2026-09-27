package com.predisched.benchmark.suite;

import java.util.Arrays;

/** Mean, sample standard deviation, Welch's t-test and Cohen's d (prompt 20 summary). */
public final class Stats {

    private Stats() {}

    public static double mean(double[] x) {
        return Arrays.stream(x).average().orElse(Double.NaN);
    }

    /** Sample standard deviation (n - 1); NaN below two values. */
    public static double std(double[] x) {
        if (x.length < 2) {
            return Double.NaN;
        }
        double m = mean(x);
        return Math.sqrt(Arrays.stream(x).map(v -> (v - m) * (v - m)).sum() / (x.length - 1));
    }

    /** Two-sided p-value of Welch's unequal-variance t-test; NaN when it cannot be computed. */
    public static double welchP(double[] a, double[] b) {
        if (a.length < 2 || b.length < 2) {
            return Double.NaN;
        }
        double va = Math.pow(std(a), 2) / a.length;
        double vb = Math.pow(std(b), 2) / b.length;
        double se = va + vb;
        if (se == 0) {
            return mean(a) == mean(b) ? 1.0 : 0.0;
        }
        double t = (mean(a) - mean(b)) / Math.sqrt(se);
        double df = se * se / (va * va / (a.length - 1) + vb * vb / (b.length - 1));
        // P(|T| > |t|) = I_{df / (df + t^2)}(df / 2, 1 / 2)
        return regularizedBeta(df / (df + t * t), df / 2.0, 0.5);
    }

    /** Cohen's d with the pooled standard deviation: (mean(a) - mean(b)) / s_pooled. */
    public static double cohensD(double[] a, double[] b) {
        if (a.length < 2 || b.length < 2) {
            return Double.NaN;
        }
        double pooled = Math.sqrt(((a.length - 1) * Math.pow(std(a), 2)
                + (b.length - 1) * Math.pow(std(b), 2)) / (a.length + b.length - 2));
        return pooled == 0 ? 0 : (mean(a) - mean(b)) / pooled;
    }

    /** I_x(a, b) by the continued fraction (Numerical Recipes 6.4). */
    static double regularizedBeta(double x, double a, double b) {
        if (x <= 0) {
            return 0;
        }
        if (x >= 1) {
            return 1;
        }
        double front = Math.exp(logGamma(a + b) - logGamma(a) - logGamma(b)
                + a * Math.log(x) + b * Math.log(1 - x));
        if (x < (a + 1) / (a + b + 2)) {
            return front * betaFraction(x, a, b) / a;
        }
        return 1 - front * betaFraction(1 - x, b, a) / b;
    }

    private static double betaFraction(double x, double a, double b) {
        final double tiny = 1e-300;
        double c = 1;
        double d = 1 - (a + b) * x / (a + 1);
        d = Math.abs(d) < tiny ? tiny : d;
        d = 1 / d;
        double h = d;
        for (int m = 1; m <= 300; m++) {
            int m2 = 2 * m;
            double aa = m * (b - m) * x / ((a + m2 - 1) * (a + m2));
            d = 1 + aa * d;
            d = Math.abs(d) < tiny ? tiny : d;
            c = 1 + aa / c;
            c = Math.abs(c) < tiny ? tiny : c;
            d = 1 / d;
            h *= d * c;
            aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1));
            d = 1 + aa * d;
            d = Math.abs(d) < tiny ? tiny : d;
            c = 1 + aa / c;
            c = Math.abs(c) < tiny ? tiny : c;
            d = 1 / d;
            double delta = d * c;
            h *= delta;
            if (Math.abs(delta - 1) < 1e-12) {
                break;
            }
        }
        return h;
    }

    /** Lanczos approximation of ln Γ(x). */
    static double logGamma(double x) {
        double[] g = {76.18009172947146, -86.50532032941677, 24.01409824083091,
            -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5};
        double y = x;
        double tmp = x + 5.5;
        tmp -= (x + 0.5) * Math.log(tmp);
        double ser = 1.000000000190015;
        for (double c : g) {
            ser += c / ++y;
        }
        return -tmp + Math.log(2.5066282746310005 * ser / x);
    }
}
