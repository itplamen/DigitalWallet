package com.itplamen.digitalwallet.transfer.controller;

import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.dto.TransferResponse;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.service.TransferService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@Validated
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

    private final TransferService transferService;

    @PostMapping
    public Mono<ResponseEntity<TransferResponse>> initiateTransfer(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 255) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {
        return transferService.initiateTransfer(idempotencyKey, request)
                .map(result -> ResponseEntity
                        .status(result.responseStatus())
                        .body(result.response()));
    }

    @GetMapping("/{transferId}")
    public Mono<Transfer> getTransfer(@PathVariable UUID transferId) {
        return transferService.getTransfer(transferId);
    }
}