package com.itplamen.digitalwallet.transfer.dto;

import com.itplamen.digitalwallet.transfer.entity.Transfer;

public record TransferInitiationResult(Transfer transfer, int responseStatus) {}
