package com.itplamen.digitalwallet.account.controller;

import com.itplamen.digitalwallet.account.dto.AccountEligibilityResponse;
import com.itplamen.digitalwallet.account.dto.CreateAccountRequest;
import com.itplamen.digitalwallet.account.dto.UpdateAccountStatusRequest;
import com.itplamen.digitalwallet.account.entity.Account;
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

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    @PostMapping
    public Mono<ResponseEntity<Account>> create(@Valid @RequestBody CreateAccountRequest request) {
        return accountService.create(request.accountNumber())
                .map(account -> ResponseEntity.created(URI.create("/api/v1/accounts/" + account.getId()))
                        .body(account));
    }

    @GetMapping("/{accountId}")
    public Mono<Account> get(@PathVariable Long accountId) {
        return accountService.get(accountId);
    }

    @PutMapping("/{accountId}/status")
    public Mono<Account> updateStatus(
            @PathVariable Long accountId,
            @Valid @RequestBody UpdateAccountStatusRequest request
    ) {
        return accountService.updateStatus(accountId, request.status());
    }

    @GetMapping("/{accountId}/eligibility")
    public Mono<ResponseEntity<AccountEligibilityResponse>> checkEligibility(@PathVariable Long accountId) {
        return accountService.checkEligibility(accountId)
                .map(response -> ResponseEntity.status(
                                "NOT_FOUND".equals(response.reason()) ? HttpStatus.NOT_FOUND : HttpStatus.OK)
                        .body(response));
    }
}
