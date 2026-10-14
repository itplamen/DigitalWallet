package com.itplamen.digitalwallet.transfer.controller;

import com.itplamen.digitalwallet.transfer.dto.TransferRequest;
import com.itplamen.digitalwallet.transfer.entity.Transfer;
import com.itplamen.digitalwallet.transfer.service.TransferService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

    private final TransferService transferService;

    @PostMapping
    public Mono<ResponseEntity<Transfer>> initiateTransfer(@Valid @RequestBody TransferRequest request) {
        return transferService.initiateTransfer(request)
                .map(result -> ResponseEntity
                        .status(result.created() ? HttpStatus.CREATED : HttpStatus.OK)
                        .body(result.transfer()));
    }

    @GetMapping("/{transferId}")
    public Mono<Transfer> getTransfer(@PathVariable Long transferId) {
        return transferService.getTransfer(transferId);
    }
}