package com.itplamen.digitalwallet.account.mapper;

import com.itplamen.digitalwallet.account.dto.AccountResponse;
import com.itplamen.digitalwallet.account.entity.Account;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface AccountMapper {

    AccountResponse toResponse(Account account);
}
