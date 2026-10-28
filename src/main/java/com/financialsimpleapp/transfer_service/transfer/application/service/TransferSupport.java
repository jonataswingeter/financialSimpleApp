package com.financialsimpleapp.transfer_service.transfer.application.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

public final class TransferSupport {

  private TransferSupport() {
    // Evita instanciação de classe utilitária/suporte
  }

  public static void validateIdempotencyKey(String key) {
    if (key == null || key.isBlank() || key.length() > 255) {
      throw new IllegalArgumentException(
          "Idempotency key must contain between 1 and 255 characters");
    }
  }

  public static BigDecimal normalizeAmount(BigDecimal amount) {
    if (amount == null) {
      throw new IllegalArgumentException("Amount is required");
    }

    try {
      return amount.setScale(2, RoundingMode.UNNECESSARY);
    } catch (ArithmeticException exception) {
      throw new IllegalArgumentException(
          "Amount cannot have more than 2 decimal places", exception);
    }
  }

  public static String calculateRequestHash(
      UUID fromAccountId, UUID toAccountId, BigDecimal amount, String currency) {
    String canonicalRequest =
        String.join(
            "\n",
            "transfer-v1",
            fromAccountId.toString(),
            toAccountId.toString(),
            amount.toPlainString(),
            currency);

    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonicalRequest.getBytes(StandardCharsets.UTF_8));

      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is not available", exception);
    }
  }
}
