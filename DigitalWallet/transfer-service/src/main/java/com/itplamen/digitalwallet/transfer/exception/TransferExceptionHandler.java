package com.itplamen.digitalwallet.transfer.exception;

import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;

import java.util.stream.Collectors;

@RestControllerAdvice
public class TransferExceptionHandler {

    @ExceptionHandler(WebExchangeBindException.class)
    public ProblemDetail handleValidation(WebExchangeBindException exception) {
        String detail = exception.getAllErrors().stream()
                .map(this::formatValidationError)
                .distinct()
                .collect(Collectors.joining("; "));
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
    }

    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail handleIdempotencyConflict(IdempotencyConflictException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    }

    private String formatValidationError(ObjectError error) {
        String field = error instanceof FieldError fieldError ? fieldError.getField() : "request";
        return field + ": " + error.getDefaultMessage();
    }

    @ExceptionHandler(InvalidTransferStatusTransitionException.class)
    public ProblemDetail handleInvalidTransition(InvalidTransferStatusTransitionException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(TransferNotFoundException.class)
    public ProblemDetail handleNotFound(TransferNotFoundException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, exception.getMessage());
    }

}
