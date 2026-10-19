package com.itplamen.digitalwallet.transfer.dto;

import com.itplamen.digitalwallet.transfer.entity.TransferStatus;

import java.util.UUID;

public record TransferResponse(UUID transferId, TransferStatus status) { }
