package com.itplamen.digitalwallet.wallet.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.itplamen.digitalwallet.wallet.entity.LedgerEntry;
import com.itplamen.digitalwallet.wallet.entity.OutboxMessage;
import com.itplamen.digitalwallet.wallet.entity.WalletTransferResult;
import com.itplamen.digitalwallet.wallet.repository.LedgerEntryRepository;
import com.itplamen.digitalwallet.wallet.repository.OutboxMessageRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletRepository;
import com.itplamen.digitalwallet.wallet.repository.WalletTransferResultRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletService {

    private final ObjectMapper objectMapper;
    private final WalletRepository walletRepository;
    private final TransactionRepository transactionRepository;
    private final OutboxMessageRepository outboxRepository;
    private final TransactionalOperator transactionalOperator;

    public Mono<Void> processTransferDebit(TransferEventPayload payload) {
        return walletRepository.findByUserId(payload.sourceUserId())
                .flatMap(wallet -> {
                    if (wallet.getBalance().compareTo(payload.amount()) < 0) {
                        log.warn("Insufficient funds for user ID: {}. Current balance: {}, Required: {}",
                                payload.sourceUserId(), wallet.getBalance(), payload.amount());
                        return saveFailedTransactionAndOutbox(wallet.getId(), payload.transferId(), payload.amount(), "INSUFFICIENT_FUNDS");
                    }

                    wallet.setBalance(wallet.getBalance().subtract(payload.amount()));
                    wallet.setUpdatedAt(Instant.now());

                    return walletRepository.save(wallet)
                            .flatMap(savedWallet ->
                                    saveSuccessTransactionAndOutbox(savedWallet.getId(), payload.transferId(), payload.amount())
                            );
                })
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("Wallet not found for user ID: {}", payload.sourceUserId());
                    return saveFailedTransactionAndOutbox(null, payload.transferId(), payload.amount(), "WALLET_NOT_FOUND");
                }))
                .as(transactionalOperator::transactional);
    }

    private Mono<Void> saveSuccessTransactionAndOutbox(Long walletId, Long transferId, BigDecimal amount) {
        Transaction tx = Transaction.builder()
                .walletId(walletId)
                .transferId(transferId)
                .amount(amount)
                .type("DEBIT")
                .status("SUCCESS")
                .createdAt(Instant.now())
                .build();

        return transactionRepository.save(tx)
                .then(saveOutbox(transferId, "WALLET_DEBITED", Map.of("transferId", transferId, "status", "SUCCESS")));
    }

    private Mono<Void> saveFailedTransactionAndOutbox(Long walletId, Long transferId, BigDecimal amount, String reason) {
        if (walletId != null) {
            Transaction tx = Transaction.builder()
                    .walletId(walletId)
                    .transferId(transferId)
                    .amount(amount)
                    .type("DEBIT")
                    .status("FAILED")
                    .createdAt(Instant.now())
                    .build();

            return transactionRepository.save(tx)
                    .then(saveOutbox(transferId, "WALLET_DEBIT_FAILED", Map.of("transferId", transferId, "reason", reason)));
        }

        return saveOutbox(transferId, "WALLET_DEBIT_FAILED", Map.of("transferId", transferId, "reason", reason));
    }

    private Mono<Void> saveOutbox(Long transferId, String eventType, Map<String, Object> payloadMap) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(payloadMap);
            OutboxMessage outbox = OutboxMessage.builder()
                    .aggregateType("WALLET")
                    .aggregateId(transferId.toString())
                    .eventType(eventType)
                    .payload(jsonPayload)
                    .createdAt(Instant.now())
                    .processed(false)
                    .version(0L)
                    .build();

            return outboxRepository.save(outbox).then();
        } catch (Exception e) {
            log.error("Failed to serialize outbox payload for transfer ID: {}", transferId, e);
            return Mono.error(e);
        }
    }

    public record TransferEventPayload(
            Long transferId,
            Long sourceUserId,
            Long targetUserId,
            BigDecimal amount,
            String currency
    ) {}
}