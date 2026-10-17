package com.itplamen.digitalwallet.account.controller;

import com.itplamen.digitalwallet.account.dto.AccountEligibilityResponse;
import com.itplamen.digitalwallet.account.dto.AccountEligibilityReason;
import com.itplamen.digitalwallet.account.dto.AccountResponse;
import com.itplamen.digitalwallet.account.dto.CreateAccountRequest;
import com.itplamen.digitalwallet.account.dto.UpdateAccountStatusRequest;
import com.itplamen.digitalwallet.account.mapper.AccountMapper;
import com.itplamen.digitalwallet.account.service.AccountService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;
    private final AccountMapper accountMapper;

    @PostMapping
    public Mono<ResponseEntity<AccountResponse>> create(@Valid @RequestBody CreateAccountRequest request) {
        return accountService.create(request.accountNumber(), request.customerName(), request.email())
                .map(account -> ResponseEntity.created(URI.create("/api/v1/accounts/" + account.getId()))
                        .body(accountMapper.toResponse(account)));
    }

    @GetMapping("/{accountId}")
    public Mono<AccountResponse> get(@PathVariable UUID accountId) {
        return accountService.get(accountId).map(accountMapper::toResponse);
    }

    @PutMapping("/{accountId}/status")
    public Mono<AccountResponse> updateStatus(
            @PathVariable UUID accountId,
            @Valid @RequestBody UpdateAccountStatusRequest request) {
        return accountService.updateStatus(accountId, request.status()).map(accountMapper::toResponse);
    }

    @GetMapping("/{accountId}/eligibility")
    public Mono<ResponseEntity<AccountEligibilityResponse>> checkEligibility(@PathVariable UUID accountId) {
        return accountService.checkEligibility(accountId)
                .map(response -> ResponseEntity.status(
                                AccountEligibilityReason.NOT_FOUND.name().equals(response.reason())
                                        ? HttpStatus.NOT_FOUND
                                        : HttpStatus.OK)
                        .body(response));
    }

}
