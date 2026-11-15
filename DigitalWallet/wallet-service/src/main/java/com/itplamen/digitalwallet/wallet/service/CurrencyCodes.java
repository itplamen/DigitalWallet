package com.itplamen.digitalwallet.wallet.service;

import java.util.Locale;
import java.util.regex.Pattern;

final class CurrencyCodes {

    private static final Pattern CURRENCY_PATTERN = Pattern.compile("[A-Za-z]{3}");

    private CurrencyCodes() { }

    static boolean isValid(String currency) {
        return currency != null && CURRENCY_PATTERN.matcher(currency).matches();
    }

    static String normalize(String currency) {
        return currency.toUpperCase(Locale.ROOT);
    }
}
